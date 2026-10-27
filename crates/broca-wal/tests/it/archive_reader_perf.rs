//! Opt-in benchmark against the real archive codec. It prints read counts and timings and
//! never fails on them; its only assertions are the payload checks in `sweep`.
use broca_wal::archive::{ArchiveReader, ArchiveWriter};
use std::{
    cell::Cell,
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
    rc::Rc,
    time::Instant,
};

struct Counted {
    file: File,
    calls: Rc<Cell<u64>>,
    bytes: Rc<Cell<u64>>,
}
impl Read for Counted {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.file.read(buf)?;
        self.calls.set(self.calls.get() + 1);
        self.bytes.set(self.bytes.get() + n as u64);
        Ok(n)
    }
}
impl Seek for Counted {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

fn sweep<R: Read + Seek>(mut reader: ArchiveReader<R>, n: u64, payload: &[u8]) {
    for i in (0..n).rev() {
        assert_eq!(
            reader
                .read_member(&format!("{i:016x}"))
                .unwrap()
                .unwrap()
                .payload,
            payload
        );
    }
}

/// Benchmark, not a correctness test: sweeps 1,024 and 4,096 production-median members through
/// unbuffered, buffered and `ArchiveReader::open` readers. The cache and read-cost guarantees
/// are asserted by the archive cache tests in the library.
#[test]
#[ignore = "benchmark: archive reader sweep; run with --release --ignored --nocapture"]
fn archive_reader_population_sweep() {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-archive-perf-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    // THE PRODUCTION MEDIAN, measured over 35,050 content-bearing WALs -- not a round
    // synthetic. A 1 KiB payload made 64 KiB read-ahead look maximally wasteful, which is
    // the wrong way to be wrong about a buffering decision.
    //
    // Raising this alone USED TO REFUSE: the per-member read limit below was pinned to the
    // same 1024, so a bigger payload tripped "archive member exceeds per-member read limit"
    // and the harness looked broken rather than misconfigured. The limit is now independent
    // of the payload, as it is in production (DEFAULT_MAX_READ_BYTES, 1 GiB).
    let payload = vec![0x5a; 28_081];
    for n in [1024, 4096] {
        let path = dir.join(format!("fold-{n}.ark"));
        let mut writer = ArchiveWriter::create(&path)
            .unwrap()
            .without_feature_floor();
        for i in 0..n {
            writer.append(&format!("{i:016x}"), &payload).unwrap();
        }
        writer.seal().unwrap();
        for buffered in [false, true] {
            let calls = Rc::new(Cell::new(0));
            let bytes = Rc::new(Cell::new(0));
            let source = Counted {
                file: File::open(&path).unwrap(),
                calls: calls.clone(),
                bytes: bytes.clone(),
            };
            let start = Instant::now();
            if buffered {
                sweep(
                    ArchiveReader::new(BufReader::with_capacity(64 * 1024, source), 1 << 20),
                    n,
                    &payload,
                );
            } else {
                sweep(ArchiveReader::new(source, 1 << 20), n, &payload);
            }
            eprintln!(
                "n={n} buffered={buffered} reads={} bytes={} elapsed={:?}",
                calls.get(),
                bytes.get(),
                start.elapsed()
            );
        }
        let start = Instant::now();
        sweep(ArchiveReader::open(&path, 1024).unwrap(), n, &payload);
        eprintln!("n={n} production_open elapsed={:?}", start.elapsed());
    }
    std::fs::remove_dir_all(dir).unwrap();
}
