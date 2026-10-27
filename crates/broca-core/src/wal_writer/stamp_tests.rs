//! Version 4 writer stamps: the header's `appended_ms` and `writer_id` fields, which only an
//! overhaul root (`broca-state/2`) writes.
//!
//! Reading the stored bytes back through `FileWal::open` truncates a torn tail before it returns,
//! so a frame cut inside its header that reads back as absent is the crash-cut contract: recovery
//! discards it and the log stays usable.

use super::*;
use broca_protocol::StepId;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One session's store on a scratch state root, marked `broca-state/2` unless told otherwise.
struct Root {
    state_root: PathBuf,
    wal_store: FileWalStore,
    session: BindIdentity,
}

impl Root {
    fn new(name: &str, overhaul: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let state_root = broca_test_support::scratch_root().join(format!(
            "wal-stamp-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(state_root.join("wal")).unwrap();
        if overhaul {
            std::fs::write(
                state_root.join(crate::state_format::MARKER_FILENAME),
                b"broca-state/2\n",
            )
            .unwrap();
        }
        Self {
            wal_store: FileWalStore::new(state_root.join("wal")),
            session: BindIdentity::new("/stamp-test", "runner", name),
            state_root,
        }
    }

    fn log_path(&self) -> PathBuf {
        self.wal_store.wal_path(&self.session)
    }

    fn floor_path(&self) -> PathBuf {
        self.wal_store.feature_floor_path()
    }

    /// The writer WAL a handle appends through, opened as the daemon's store opens it, with the
    /// feature floor beside the state root.
    fn open(&self) -> Box<dyn Wal + Send> {
        self.wal_store
            .open_with_class(&self.session, None, None)
            .expect("open the log")
    }

    /// A handle whose writer thread appends at `fence`.
    fn handle(&self, fence: u64) -> WalHandle {
        WalHandle::spawn(self.open(), fence).expect("spawn the writer")
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.state_root);
    }
}

fn record(n: u64) -> WalRecord {
    WalRecord::TurnFinished { step_id: StepId(n) }
}

fn stored(root: &Root) -> Vec<u8> {
    std::fs::read(root.log_path()).unwrap()
}

/// Every complete version 4 frame of the log, in order. A lineage frame carries the log's
/// identity rather than a record, and every append is trailed by one, so it is skipped.
fn frames(root: &Root) -> Vec<broca_wal::framing::v4::Header> {
    let bytes = stored(root);
    let mut headers = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        if bytes[offset + 4] == broca_wal::framing::V4_FRAMING_VERSION {
            let header = broca_wal::framing::v4::decode_header(&bytes[offset..])
                .unwrap()
                .expect("a complete frame has its header");
            offset += header.frame_len();
            headers.push(header);
        } else {
            // Lineage frames: the version 2 header plus 16 identity bytes.
            assert_eq!(len, 16, "only a lineage frame is not a version 4 frame");
            offset += broca_wal::framing::HEADER_LEN + len;
        }
    }
    headers
}

/// Every record the writer's own decoder reads back from the log.
fn read_log(root: &Root) -> Vec<broca_wal::FramedRecord> {
    let bytes = stored(root);
    let mut records = Vec::new();
    let mut offset = 0;
    loop {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                offset += consumed;
                records.push(*framed);
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            broca_wal::framing::DecodeStep::TornTail => break,
            other => panic!(
                "the writer's own reader refused its log at offset {offset}: {}",
                match other {
                    broca_wal::framing::DecodeStep::Corrupt(error) => error.to_string(),
                    broca_wal::framing::DecodeStep::NewerWriter { missing, .. } =>
                        format!("newer writer, missing {}", missing.join(", ")),
                    _ => "unexpected step".to_owned(),
                }
            ),
        }
    }
    records
}

/// Where a freshly created log's identity frame ends and its first record frame begins.
const FIRST_FRAME: usize = broca_wal::framing::LINEAGE_FRAME_LEN;

/// Where the second record frame begins: past the first frame and the identity frame each
/// append trails it with. A cut must land here to be a cut inside a record header rather than
/// in the identity frame that precedes it.
fn second_frame_start(log: &[u8]) -> usize {
    FIRST_FRAME
        + broca_wal::framing::v4::decode_header(&log[FIRST_FRAME..])
            .unwrap()
            .expect("the first frame has a header")
            .frame_len()
        + broca_wal::framing::LINEAGE_FRAME_LEN
}

/// Keep a writer inclusive timeout while the writer WAL holds the loop, so a broken gate fails the
/// test instead of hanging it.
fn gate_deadline() -> std::time::Duration {
    std::time::Duration::from_secs(20)
}

/// A writer WAL that holds the writer thread in its first `append`, so a test can see everything
/// enqueue finished before the writer ran.
struct GatedWal {
    inner: Box<dyn Wal + Send>,
    gate: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl GatedWal {
    fn pass_gate(&self) {
        // Only the first append waits: the gate receiver is consumed once and later calls find
        // nothing left to receive.
        let Ok(gate) = self.gate.try_lock() else {
            return;
        };
        let _ = gate.recv_timeout(gate_deadline());
    }
}

impl Wal for GatedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.pass_gate();
        self.inner.append(fence, record, sync)
    }

    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.pass_gate();
        self.inner.append_preencoded(fence, record, sync, frame)
    }

    fn on_overhaul_root(&self) -> bool {
        self.inner.on_overhaul_root()
    }

    fn state_root_path(&self) -> Option<PathBuf> {
        self.inner.state_root_path()
    }

    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        self.inner.append_budget()
    }

    fn recovery_byte_cap(&self) -> Option<u64> {
        self.inner.recovery_byte_cap()
    }

    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.inner.append_budget_scope()
    }

    fn sync(&self) -> Result<(), WalError> {
        self.inner.sync()
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.inner.replay()
    }

    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.inner.tail_after_seq(after_seq)
    }

    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }

    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
}

#[tokio::test]
async fn overhaul_root_frames_carry_a_writer_stamp() {
    let root = Root::new("stamped", true);
    let handle = root.handle(7);
    handle.record(record(1)).into_result().unwrap();
    handle.record(record(2)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");

    let landed = frames(&root);
    assert_eq!(landed.len(), 2);
    for (index, header) in landed.iter().enumerate() {
        assert_eq!(header.metadata.seq, index as u64 + 1);
        assert_eq!(header.metadata.fence, 7, "the lease tenure stays the fence");
        assert!(
            header.metadata.appended_ms > 0,
            "a frame on an overhaul root carries its write time"
        );
        assert_ne!(header.metadata.writer_id, [0; 16]);
        assert_eq!(header.requires, ["frame-stamps/v1"]);
    }
    assert_eq!(
        landed[0].metadata.writer_id, landed[1].metadata.writer_id,
        "one writer stamps its id on every frame it appends"
    );
    // The writer's own reader accepts what it wrote.
    let read = read_log(&root);
    assert_eq!(read.len(), 2);
}

#[tokio::test]
async fn a_legacy_root_keeps_the_frames_it_wrote_before() {
    let root = Root::new("legacy", false);
    let handle = root.handle(7);
    handle.record(record(1)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");

    let bytes = stored(&root);
    assert_ne!(
        bytes[FIRST_FRAME + 4],
        broca_wal::framing::V4_FRAMING_VERSION,
        "a root without the overhaul marker keeps its legacy frame version"
    );
    assert!(
        !root.floor_path().exists(),
        "a legacy frame names no new feature, so nothing is raised at the floor"
    );
    assert_eq!(read_log(&root).len(), 1);
}

#[tokio::test]
async fn appended_ms_monotonic_per_process() {
    let root = Root::new("monotonic", true);
    let handle = root.handle(3);
    for n in 1..=20 {
        handle.record(record(n)).into_result().unwrap();
        handle.sync().await.unwrap();
        let stamped = frames(&root);
        assert_eq!(stamped.len() as u64, n);
        // This writer's frames. Sibling tests in the same binary stamp their own, and the
        // process-wide floor is shared, so only one writer's sequence says anything here.
        let writer_id = stamped[0].metadata.writer_id;
        let mut last = 0;
        for header in stamped.iter().filter(|h| h.metadata.writer_id == writer_id) {
            assert!(
                header.metadata.appended_ms >= last,
                "appended_ms went backwards: {} after {last}",
                header.metadata.appended_ms
            );
            last = header.metadata.appended_ms;
        }
    }
    handle.shutdown().await.expect("shutdown");
}

/// A writer WAL that reports, for each frame, the monotonic reading its own writer thread had
/// just taken. Comparing that with the frame's `appended_ms` shows where the stamp came from.
struct StampedWal {
    inner: Box<dyn Wal + Send>,
    readings: std::sync::Arc<Mutex<Vec<WriterReading>>>,
}

/// The wall-clock time the writer thread read just before it handed a frame to the WAL.
struct WriterReading {
    wall_ms: u128,
}

impl StampedWal {
    fn note(&self) {
        let wall_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        self.readings
            .lock()
            .unwrap()
            .push(WriterReading { wall_ms });
    }
}

impl Wal for StampedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.note();
        self.inner.append(fence, record, sync)
    }

    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.note();
        self.inner.append_preencoded(fence, record, sync, frame)
    }

    fn on_overhaul_root(&self) -> bool {
        self.inner.on_overhaul_root()
    }

    fn state_root_path(&self) -> Option<PathBuf> {
        self.inner.state_root_path()
    }

    fn append_budget(&self) -> Option<(u64, u64, u64)> {
        self.inner.append_budget()
    }

    fn recovery_byte_cap(&self) -> Option<u64> {
        self.inner.recovery_byte_cap()
    }

    fn append_budget_scope(&self) -> broca_wal::size_limit::SizeScope {
        self.inner.append_budget_scope()
    }

    fn sync(&self) -> Result<(), WalError> {
        self.inner.sync()
    }

    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.inner.replay()
    }

    fn tail_after_seq(&self, after_seq: u64) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
        self.inner.tail_after_seq(after_seq)
    }

    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }

    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
}

#[tokio::test]
async fn appended_ms_taken_on_writer_thread() {
    let root = Root::new("writer-thread", true);
    // Each frame records the writer thread's monotonic reading taken when it assembled the
    // frame's header. A handle stamps `appended_ms` from the same place, so it is at or after
    // that reading; one sampled while the record was enqueued, before the writer ran, is not.
    let readings = std::sync::Arc::new(Mutex::new(Vec::new()));
    let record_readings = readings.clone();
    let handle = WalHandle::spawn(
        Box::new(StampedWal {
            inner: root.open(),
            readings: record_readings,
        }),
        3,
    )
    .expect("spawn the writer");
    handle.record(record(1)).into_result().unwrap();
    handle.sync().await.expect("sync");
    handle.record(record(2)).into_result().unwrap();
    handle.sync().await.expect("sync");
    let landed = frames(&root);
    assert_eq!(landed.len(), 2);
    {
        let readings = readings.lock().unwrap();
        assert_eq!(readings.len(), 2);
        for (header, reading) in landed.iter().zip(readings.iter()) {
            assert!(
                header.metadata.appended_ms as u128 >= reading.wall_ms,
                "appended_ms {} predates the writer thread's own reading at {}: it was taken \
                 before the writer ran",
                header.metadata.appended_ms,
                reading.wall_ms
            );
        }
    }
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn preencoded_frame_hash_off_thread() {
    let root = Root::new("hash-off-thread", true);
    let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
    let handle = WalHandle::spawn(
        Box::new(GatedWal {
            inner: root.open(),
            gate: Mutex::new(gate_rx),
        }),
        3,
    )
    .expect("spawn the writer");
    // The writer is parked in its first append, and enqueue has already returned. Everything
    // enqueue settles off the writer thread is settled now: the feature floor names what every
    // version 4 frame requires, while the log still holds no frame.
    handle.record(record(1)).into_result().unwrap();
    assert_eq!(
        broca_wal::read_floor(&root.floor_path()).unwrap(),
        std::collections::BTreeSet::from(["frame-stamps/v1".to_owned()]),
        "enqueue settles the frame's feature names before the writer thread runs"
    );
    assert_eq!(stored(&root).len(), FIRST_FRAME);
    gate_tx.send(()).unwrap();
    handle.sync().await.expect("sync past the gate");
    assert_eq!(frames(&root).len(), 1);
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn crash_cut_inside_a_version_4_header_is_a_torn_tail() {
    let root = Root::new("crash-cut", true);
    let handle = root.handle(5);
    handle.record(record(1)).into_result().unwrap();
    handle.record(record(2)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");
    let complete = stored(&root);
    assert_eq!(frames(&root).len(), 2);

    // A crash mid-write leaves the first frame and its trailing identity frame whole, and cuts
    // the second frame's header partway. (A reader without this header treats the bytes after
    // the first frame as a torn tail, so the cut must come after them.)
    let second_start = second_frame_start(&complete);
    let second = broca_wal::framing::v4::decode_header(&complete[second_start..])
        .unwrap()
        .expect("the second frame has a header");
    // A version 4 header is 83 bytes before its requirements array. The cut is past the legacy
    // header length recovery first reads, so what survives is a partially written version 4
    // header rather than an empty read.
    let cut = second_start + 64;
    assert!(cut < second_start + second.frame_len());
    std::fs::write(root.log_path(), &complete[..cut]).unwrap();

    let recovered = root.open();
    assert_eq!(
        recovered.last_seq(),
        1,
        "recovery discards the header that was cut short"
    );
    // Recovery may discard the torn bytes; what it may not do is keep the first frame while
    // failing to read it. A log that reopened at sequence 1 and holds no later frame is a log
    // that ends at its last complete frame.
    assert_eq!(frames(&root).len(), 1, "only the complete frame survives");
    // Appending after recovery continues from the surviving frame.
    let handle = WalHandle::spawn(recovered, 5).expect("spawn on the recovered log");
    handle.record(record(3)).into_result().unwrap();
    handle.record(record(4)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");
    let landed = frames(&root);
    assert_eq!(landed.len(), 3);
    assert_eq!(landed[1].metadata.seq, 2);
    assert_eq!(landed[2].metadata.seq, 3);
}

#[tokio::test]
async fn power_loss_cut_inside_a_version_4_header_is_a_torn_tail() {
    let root = Root::new("power-loss", true);
    let handle = root.handle(5);
    handle.record(record(1)).into_result().unwrap();
    handle.record(record(2)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");
    let complete = stored(&root);

    // A power loss that reached the disk partway through the second frame's header.
    let cut = second_frame_start(&complete) + 64;
    assert!(cut < complete.len());
    std::fs::write(root.log_path(), &complete[..cut]).unwrap();

    let recovered = root.open();
    assert_eq!(
        recovered.last_seq(),
        1,
        "only the frame that reached the disk in full survives"
    );
    assert_eq!(frames(&root).len(), 1);
    assert_eq!(read_log(&root).len(), 1);
}
