//! Tests of the writer's v2 seal: the index length arithmetic, raising the WAL feature floor
//! before the index is written, and what a crash part way through a seal can leave.
use super::v2_tests::{kind, Kind};
use super::*;
use std::{
    io::Cursor,
    sync::atomic::{AtomicU64, Ordering},
};

fn addr(n: u64) -> String {
    format!("{n:016x}")
}

fn scratch(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-archive-writer-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn members(count: u64) -> Vec<(String, Vec<u8>)> {
    (0..count)
        .map(|n| {
            (
                addr(n.wrapping_mul(0x9e37_79b9) & 0xffff_ffff),
                vec![n as u8; 20 + (n as usize * 53) % 300],
            )
        })
        .collect()
}

/// Seal `members` with the production writer and return the container's bytes.
fn sealed(dir: &Path, stamp: u64, members: &[(String, Vec<u8>)]) -> Vec<u8> {
    let path = dir.join(format!("fold-{stamp}.ark"));
    let mut writer = ArchiveWriter::create(&path)
        .unwrap()
        .without_feature_floor();
    for (name, payload) in members {
        writer.append(name, payload).unwrap();
    }
    writer.seal().unwrap();
    std::fs::read(path).unwrap()
}

#[test]
fn the_index_length_is_checked_against_the_u32_trailer() {
    // 4 + 32 * count + 64 must fit a u32: the last count that does, and the first that
    // does not, including counts whose products overflow before the comparison.
    let last = ((u32::MAX as u64 - 68) / 32) as usize;
    assert_eq!(v2_index_len(last), Some(4 + 32 * last as u32 + 64));
    assert_eq!(v2_index_len(last + 1), None);
    assert_eq!(v2_index_len(0), Some(68));
    assert_eq!(v2_index_len(usize::MAX), None);
    assert_eq!(v2_index_len(usize::MAX / 32 + 1), None);
}

#[test]
fn a_container_must_be_named_by_its_stamp() {
    let dir = scratch("names");
    for name in ["container.ark", "fold-01.ark", "fold-1.journal"] {
        let error = ArchiveWriter::create(dir.join(name)).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{name}");
        assert!(!dir.join(name).exists(), "{name}");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// The seal sorts entries by address and the result verifies under the stamp in the name.
#[test]
fn the_seal_writes_a_sorted_verified_v2_index() {
    let dir = scratch("sorted");
    let members = members(40);
    let bytes = sealed(&dir, 77, &members);
    assert_eq!(kind(&bytes, 77), Kind::Verified);
    assert_eq!(bytes, super::v2_tests::v2_container(77, &members));
    let mut reader = ArchiveReader::new(Cursor::new(bytes), 1 << 20).with_stamp(77);
    let (end, index_len) = reader.sealed_trailer().unwrap();
    assert!(matches!(
        reader.load_index(end, index_len).unwrap(),
        IndexOutcome::Verified {
            sorted_on_disk: true,
            ..
        }
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

/// The floor is raised, durably, before the index is written: a seal that cannot raise it
/// leaves the container unsealed. A writer that never chose what to do about the floor
/// refuses to seal at all.
#[test]
fn the_seal_raises_the_feature_floor_first() {
    let dir = scratch("floor");
    let floor = dir.join(crate::FEATURE_FLOOR_FILENAME);
    std::fs::write(&floor, "scope/v1\n").unwrap();
    let path = dir.join("fold-1.ark");
    let mut writer = ArchiveWriter::create(&path)
        .unwrap()
        .with_feature_floor(&floor);
    writer.append(&addr(1), b"history").unwrap();
    writer.seal().unwrap();
    assert_eq!(
        std::fs::read_to_string(&floor).unwrap(),
        "archive-index/v2\nscope/v1\n"
    );
    assert_eq!(kind(&std::fs::read(&path).unwrap(), 1), Kind::Verified);

    // A floor that cannot be raised (a directory stands at its path) fails the seal before
    // any index byte is written.
    let blocked = dir.join("blocked");
    std::fs::create_dir_all(blocked.join(crate::FEATURE_FLOOR_FILENAME)).unwrap();
    let path = dir.join("fold-2.ark");
    let mut writer = ArchiveWriter::create(&path)
        .unwrap()
        .with_feature_floor(blocked.join(crate::FEATURE_FLOOR_FILENAME));
    writer.append(&addr(1), b"history").unwrap();
    assert!(writer.seal().is_err());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 64 + 7);
    assert_eq!(
        ArchiveReader::open(&path, 100).unwrap().state().unwrap(),
        ContainerState::Unsealed
    );

    let path = dir.join("fold-3.ark");
    let mut writer = ArchiveWriter::create(&path).unwrap();
    writer.append(&addr(1), b"history").unwrap();
    assert_eq!(
        writer.seal().unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        ArchiveReader::open(&path, 100).unwrap().state().unwrap(),
        ContainerState::Unsealed
    );
    std::fs::remove_dir_all(dir).unwrap();
}

/// A seal cut short at any byte from the start of the index to the end leaves the
/// container unsealed (the trailer magic is missing, so `read_newest_member` skips it and
/// direct reads refuse it); only the full length is sealed, and its index verifies.
#[test]
fn every_prefix_of_a_seal_is_unsealed_or_fully_sealed() {
    let dir = scratch("prefix");
    let members = members(12);
    let bytes = sealed(&dir, 5, &members);
    let start = bytes.len() - 8 - (4 + 32 * members.len() + 64);
    for cut in start..bytes.len() {
        let prefix = &bytes[..cut];
        assert_eq!(kind(prefix, 5), Kind::Unsealed, "cut {cut}");
        let mut reader = ArchiveReader::new(Cursor::new(prefix.to_vec()), 1 << 20).with_stamp(5);
        for (name, _) in &members {
            assert_eq!(
                reader.read_member(name).unwrap_err().kind(),
                io::ErrorKind::PermissionDenied,
                "cut {cut}"
            );
        }
    }
    assert_eq!(kind(&bytes, 5), Kind::Verified);
    std::fs::remove_dir_all(dir).unwrap();
}

/// Page writeback order is unspecified, so the trailer can be durable while an earlier
/// page is not. With MAGIC and `index_len` present, zero or replace with stale bytes any
/// one page (4 KiB and 16 KiB) of a sealed container. The index verifies only if the page
/// missed the index and its extension entirely, and while any index still lists the
/// members, none is reported absent: each is served intact or refused with an error.
#[test]
fn a_torn_page_never_verifies_a_container_with_a_missing_member() {
    let dir = scratch("torn");
    let members = members(150);
    let bytes = sealed(&dir, 9, &members);
    // Stale bytes: what a previous, different container left at the same offsets.
    let stale = sealed(&dir, 8, &self::members(170));
    let trailer = bytes.len() - 8;
    let start = trailer - (4 + 32 * members.len() + 64);
    let mut seen = BTreeSet::new();
    for page in [4096usize, 16384] {
        for first in (0..bytes.len()).step_by(page) {
            let last = (first + page).min(trailer);
            if first >= last {
                continue;
            }
            for (label, fill) in [("zeroed", None), ("stale", Some(&stale))] {
                let mut torn = bytes.clone();
                match fill {
                    None => torn[first..last].fill(0),
                    Some(stale) => torn[first..last].copy_from_slice(&stale[first..last]),
                }
                if torn == bytes {
                    continue;
                }
                let kind = kind(&torn, 9);
                seen.insert(format!("{kind:?}"));
                let touches_index = last > start;
                if kind == Kind::Verified {
                    assert!(
                        !touches_index,
                        "{label} page {first}..{last} of {page}: a torn index verified"
                    );
                }
                let mut reader = ArchiveReader::new(Cursor::new(torn), 1 << 20).with_stamp(9);
                for (name, payload) in &members {
                    match reader.read_member(name) {
                        Ok(Some(member)) => assert_eq!(&member.payload, payload),
                        // An index that fails its structural checks leaves only the header
                        // scan, which stops at the first damaged header: members past it are
                        // missed, exactly as for a v1 container. Any index that still lists
                        // the members (verified or not) never lets one read as absent.
                        Ok(None) if kind == Kind::Invalid => {}
                        Ok(None) => panic!(
                            "{label} page {first}..{last} of {page}: {name} read as absent ({kind:?})"
                        ),
                        Err(error) => assert_eq!(error.kind(), io::ErrorKind::InvalidData),
                    }
                }
            }
        }
    }
    // Both outcomes occurred: damage outside the index left it verified, and damage to a
    // page holding index bytes did not, so neither branch above is vacuous.
    assert!(seen.contains("Verified"));
    assert!(seen.contains("Unverified") || seen.contains("Invalid"));
    std::fs::remove_dir_all(dir).unwrap();
}
