use super::*;
use std::{cell::Cell, io::Cursor, rc::Rc};

fn addr(n: u64) -> String {
    format!("{n:016x}")
}

// These tests use the v1 wire format, which writers sealed before index format v2. They pin
// how a v1 index is cached and refused, so their fixture stays v1 whatever the current
// writer seals.
fn fixture() -> Vec<u8> {
    super::v2_tests::v1_container(
        &(0..128)
            .map(|n| (addr(n), vec![n as u8; 16]))
            .collect::<Vec<_>>(),
    )
}

const INDEX: usize = 128 * (64 + 16);

#[test]
fn cached_index_refuses_all_geometry_before_publishing_a_hit() {
    for (position, replacement, message) in [
        (
            INDEX,
            127u64.to_le_bytes().to_vec()[..4].to_vec(),
            "invalid archive index count",
        ),
        (
            INDEX + 4 + 32 + 16,
            81u64.to_le_bytes().to_vec(),
            "invalid archive index coverage",
        ),
        (
            INDEX + 4 + 32,
            addr(0).into_bytes(),
            "invalid archive index coverage",
        ),
        (
            INDEX + 4 + 127 * 32 + 24,
            15u64.to_le_bytes().to_vec(),
            "archive index omits members",
        ),
    ] {
        let mut bytes = fixture();
        bytes[position..position + replacement.len()].copy_from_slice(&replacement);
        let mut reader = ArchiveReader::new(Cursor::new(bytes), 100);
        assert_eq!(reader.state().unwrap(), ContainerState::Sealed);
        assert!(reader.index.is_none(), "state must not validate the index");
        assert_eq!(reader.rescan().unwrap().len(), 128);
        assert!(reader.index.is_none(), "rescan must not validate the index");
        let (end, len) = reader.sealed_trailer().unwrap();
        for _ in 0..2 {
            assert_eq!(
                reader.indexed(&addr(0), end, len).unwrap_err().to_string(),
                message
            );
            assert!(matches!(reader.index, Some(IndexOutcome::Invalid(_))));
            // Refusing the index still permits recovery from intact member headers.
            assert_eq!(
                reader.read_member(&addr(127)).unwrap().unwrap().payload,
                vec![127; 16]
            );
        }
    }
}

#[test]
fn cached_hit_still_refuses_damaged_header_and_disagreement() {
    for position in [80, 80 + 4, 80 + 6, 80 + 8, 80 + 24, 80 + 32, 80 + 64] {
        let mut bytes = fixture();
        bytes[position] ^= 1;
        let mut reader = ArchiveReader::new(Cursor::new(bytes), 100);
        assert!(reader.read_member(&addr(0)).unwrap().is_some());
        assert!(matches!(
            reader.index,
            Some(IndexOutcome::Unverified { .. })
        ));
        for _ in 0..2 {
            assert_eq!(
                reader.read_member(&addr(1)).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }
}

#[test]
fn repeated_cached_reads_match_fresh_readers_including_recovery_and_errors() {
    let original = fixture();
    let mut variants = vec![original.clone()];
    for position in [
        INDEX,
        INDEX + 4 + 32,
        INDEX + 4 + 32 + 16,
        80,
        80 + 8,
        80 + 64,
    ] {
        let mut bytes = original.clone();
        bytes[position] ^= 1;
        variants.push(bytes);
    }
    for bytes in variants {
        let mut reader = ArchiveReader::new(Cursor::new(bytes.clone()), 100);
        for n in [127, 0, 1, 128, 2, 1, 0, 127] {
            let expected = ArchiveReader::new(Cursor::new(bytes.clone()), 100)
                .read_member(&addr(n))
                .map_err(|e| (e.kind(), e.to_string()));
            assert_eq!(
                reader
                    .read_member(&addr(n))
                    .map_err(|e| (e.kind(), e.to_string())),
                expected
            );
        }
    }
}

struct Counted {
    inner: Cursor<Vec<u8>>,
    bytes: Rc<Cell<usize>>,
}
impl Read for Counted {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes.set(self.bytes.get() + n);
        Ok(n)
    }
}
impl Seek for Counted {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

#[test]
fn repeated_hits_read_the_index_only_once() {
    let bytes = Rc::new(Cell::new(0));
    let mut reader = ArchiveReader::new(
        Counted {
            inner: Cursor::new(fixture()),
            bytes: bytes.clone(),
        },
        100,
    );
    reader.read_member(&addr(0)).unwrap().unwrap();
    let first = bytes.get();
    assert_eq!(first, 8 + 4 + 128 * 32 + 64 + 16);
    for n in 0..128 {
        assert_eq!(
            reader.read_member(&addr(n)).unwrap().unwrap().payload,
            vec![n as u8; 16]
        );
    }
    assert_eq!(bytes.get() - first, 128 * (8 + 64 + 16));
}

#[test]
fn file_open_preserves_explicit_reader_type_and_buffers_index_reads() {
    let bytes = fixture();
    let path = broca_test_support::scratch_root()
        .join(format!("broca-buffered-open-{}.ark", std::process::id()));
    std::fs::write(&path, bytes).unwrap();
    let mut reader: ArchiveReader<File> = ArchiveReader::open(&path, 100).unwrap();
    assert_eq!(reader.source.capacity(), 64 * 1024);
    assert_eq!(
        reader.read_member(&addr(127)).unwrap().unwrap().payload,
        vec![127; 16]
    );
    std::fs::remove_file(path).unwrap();
}
