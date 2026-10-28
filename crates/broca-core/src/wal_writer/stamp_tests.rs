//! Version 4 writer stamps: the header's `appended_ms` and `writer_id` fields, which only a store
//! configured at boot for an overhaul root (`broca-state/2`) writes, and the legacy-only store
//! that refuses to write on such a root.
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
        Self::with_wal_dir(name, overhaul, true)
    }

    /// A root whose `wal/` directory does not exist yet, as an overhaul root's is not when its
    /// first log is opened: the state root is marked (and may hold other files) before any log
    /// creates the directory its logs live in.
    fn without_wal_dir(name: &str, overhaul: bool) -> Self {
        Self::with_wal_dir(name, overhaul, false)
    }

    fn scratch(name: &str) -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let state_root = broca_test_support::scratch_root().join(format!(
            "wal-stamp-{}-{name}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&state_root);
        std::fs::create_dir_all(&state_root).unwrap();
        state_root
    }

    /// The store is built the way the daemon builds it: from the boot classification of the
    /// state root, so an overhaul root gets [`FileWalStore::with_config`].
    fn with_wal_dir(name: &str, overhaul: bool, create_wal_dir: bool) -> Self {
        let state_root = Self::scratch(name);
        if create_wal_dir {
            std::fs::create_dir_all(state_root.join("wal")).unwrap();
        }
        if overhaul {
            std::fs::write(
                state_root.join(broca_wal::state_format::MARKER_FILENAME),
                b"broca-state/2\n",
            )
            .unwrap();
        }
        let config = broca_wal::classify_state_root(&state_root).expect("classify the root");
        assert_eq!(config.is_overhaul(), overhaul);
        Self {
            wal_store: FileWalStore::with_config(state_root.join("wal"), config),
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
        self.spawn(self.open(), fence, StampClock::wall())
    }

    // Overhaul spawns require store context: a context-free FileWal cannot place its durable
    // writer-id/v1 marker and must refuse instead of opening a markerless writer.
    fn spawn(&self, wal: Box<dyn Wal + Send>, fence: u64, clock: StampClock) -> WalHandle {
        let (wal, seq) = WalHandle::prove_recovery(wal).unwrap();
        let context = self
            .wal_store
            .writer_marker_root()
            .map(|root| open_writers::WriterContext {
                root,
                address: session_addr(&self.session),
                identity: self.session.clone(),
                cause: None,
            });
        WalHandle::spawn_proven_with_clock(wal, seq, fence, None, None, (clock, context)).unwrap()
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

/// A writer WAL that holds the writer thread in its first version 4 append, so a test can act
/// while the writer is parked there. It reports when the writer reached the gate, and on which
/// thread.
struct GatedWal {
    inner: Box<dyn Wal + Send>,
    reached: std::sync::mpsc::Sender<std::thread::ThreadId>,
    gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl GatedWal {
    fn pass_gate(&self) {
        let _ = self.reached.send(std::thread::current().id());
        // Only the first append waits: the gate receiver is taken once.
        if let Some(gate) = self.gate.lock().unwrap().take() {
            let _ = gate.recv_timeout(gate_deadline());
        }
    }
}

impl Wal for GatedWal {
    fn writer_close_reserve(&self) -> u64 {
        self.inner.writer_close_reserve()
    }
    fn writer_id(&self) -> Option<[u8; 16]> {
        self.inner.writer_id()
    }
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        self.inner.append(fence, record, sync)
    }

    fn append_preencoded(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        frame: Vec<u8>,
    ) -> Result<u64, WalError> {
        self.inner.append_preencoded(fence, record, sync, frame)
    }

    fn append_v4(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
        prepared: broca_wal::PreparedV4Frame,
        appended_ms: u64,
    ) -> Result<u64, WalError> {
        if !matches!(
            record,
            WalRecord::WriterOpened { .. } | WalRecord::WriterClosed { .. }
        ) {
            self.pass_gate();
        }
        self.inner
            .append_v4(fence, record, sync, prepared, appended_ms)
    }

    fn wal_config(&self) -> broca_wal::WalConfig {
        self.inner.wal_config()
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
    assert!(root.wal_store.wal_config().is_overhaul());
    let handle = root.handle(7);
    handle.record(record(1)).into_result().unwrap();
    handle.record(record(2)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");

    let landed = frames(&root);
    assert_eq!(landed.len(), 4);
    for (index, header) in landed.iter().enumerate() {
        assert_eq!(header.metadata.seq, index as u64 + 1);
        assert_eq!(header.metadata.fence, 7, "the lease tenure stays the fence");
        assert!(
            header.metadata.appended_ms > 0,
            "a frame on an overhaul root carries its write time"
        );
        assert_ne!(header.metadata.writer_id, [0; 16]);
        assert_eq!(
            header.requires,
            if index == 0 || index == 3 {
                vec!["frame-stamps/v1", "writer-id/v1"]
            } else {
                vec!["frame-stamps/v1"]
            }
        );
    }
    assert!(
        landed
            .iter()
            .all(|header| header.metadata.writer_id == landed[0].metadata.writer_id),
        "one writer stamps its id on every frame it appends"
    );
    assert_eq!(
        broca_wal::read_floor(&root.floor_path()).unwrap(),
        std::collections::BTreeSet::from(["frame-stamps/v1".to_owned(), "writer-id/v1".to_owned()]),
        "the floor lists what the frames require"
    );
    // The writer's own reader accepts what it wrote.
    let read = read_log(&root);
    assert_eq!(read.len(), 4);

    // A later open is a new writer, with an id of its own.
    let handle = root.handle(8);
    handle.record(record(3)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");
    let landed = frames(&root);
    assert_eq!(landed.len(), 7);
    assert_ne!(landed[4].metadata.writer_id, landed[0].metadata.writer_id);
}

#[tokio::test]
async fn a_marked_root_without_its_wal_directory_still_writes_version_4() {
    // The state root is marked before its `wal/` directory exists, so the first log open is also
    // the call that creates `wal/`. The boot classification, not the directory, decides it.
    let root = Root::without_wal_dir("fresh-overhaul", true);
    assert!(!root.state_root.join("wal").exists());
    let handle = root.handle(7);
    handle.record(record(1)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");

    let landed = frames(&root);
    assert_eq!(landed.len(), 3);
    assert_eq!(landed[1].requires, ["frame-stamps/v1"]);
    assert!(landed[1].metadata.appended_ms > 0);
}

#[tokio::test]
async fn a_legacy_root_keeps_the_frames_it_wrote_before() {
    let root = Root::new("legacy", false);
    assert!(!root.wal_store.wal_config().is_overhaul());
    let handle = root.handle(7);
    handle.record(record(1)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");

    let bytes = stored(&root);
    let legacy = broca_wal::framing::encode(1, 7, &record(1)).unwrap();
    assert_eq!(
        bytes[FIRST_FRAME..FIRST_FRAME + legacy.len()],
        legacy,
        "a root without the overhaul marker writes the legacy frame byte for byte"
    );
    assert!(
        !root.floor_path().exists(),
        "a legacy frame names no new feature, so nothing is raised at the floor"
    );
    assert_eq!(read_log(&root).len(), 1);
}

#[test]
fn the_default_store_refuses_to_write_on_a_marked_root() {
    // `FileWalStore::new` writes only legacy frames, so on a root marked `broca-state/2`, or one
    // whose marker cannot be read, every writable open refuses by name and creates nothing.
    for marker in ["overhaul", "unreadable", "unknown"] {
        let state_root = Root::scratch("default-store");
        let marker_path = state_root.join(broca_wal::state_format::MARKER_FILENAME);
        match marker {
            "overhaul" => std::fs::write(&marker_path, b"broca-state/2\n").unwrap(),
            "unreadable" => std::fs::create_dir(&marker_path).unwrap(),
            _ => std::fs::write(&marker_path, b"broca-state/3\n").unwrap(),
        }
        let store = FileWalStore::new(state_root.join("wal"));
        assert!(!store.wal_config().is_overhaul());
        let session = BindIdentity::new("/stamp-test", "runner", "default-store");
        for open in [
            store.open(&session).map(drop),
            store.open_with_class(&session, None, None).map(drop),
            store.open_with_class(&session, Some(60), None).map(drop),
        ] {
            let error = open.expect_err(marker);
            assert!(
                matches!(
                    error,
                    WalError::StateRoot(broca_wal::StateRootError::OverhaulRootNeedsConfig { .. })
                ),
                "{marker}: {error:?}"
            );
            assert!(error.to_string().contains("wal store refused"), "{error}");
        }
        assert!(
            !state_root.join("wal").exists(),
            "{marker}: a refused open creates nothing"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }
    // The same store on an unmarked root writes as it always has.
    let state_root = Root::scratch("default-store-legacy");
    let store = FileWalStore::new(state_root.join("wal"));
    let session = BindIdentity::new("/stamp-test", "runner", "default-store");
    let mut wal = store.open(&session).expect("an unmarked root is legacy");
    wal.append(1, &record(1), true).unwrap();
    let _ = std::fs::remove_dir_all(&state_root);
}

#[tokio::test]
async fn appended_ms_is_sampled_on_the_writer_thread_when_it_writes() {
    let root = Root::new("writer-thread", true);
    // The stamp clock returns whatever the test sets, and records the thread that read it.
    let now = Arc::new(AtomicU64::new(1_000));
    let readers = Arc::new(Mutex::new(Vec::new()));
    let clock = {
        let now = now.clone();
        let readers = readers.clone();
        StampClock::from_fn(move || {
            readers.lock().unwrap().push(std::thread::current().id());
            now.load(Ordering::SeqCst)
        })
    };
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
    let handle = root.spawn(
        Box::new(GatedWal {
            inner: root.open(),
            reached: reached_tx,
            gate: Mutex::new(Some(gate_rx)),
        }),
        3,
        clock,
    );

    // The writer takes the first record, reads the clock (1000) and parks at the gate.
    handle.record(record(1)).into_result().unwrap();
    let writer_thread = reached_rx
        .recv_timeout(gate_deadline())
        .expect("the writer reaches the gate");
    assert_ne!(writer_thread, std::thread::current().id());
    // The second record is enqueued while the writer is parked, and only then does the clock
    // move. Sampling at enqueue would stamp it 1000; the writer samples 2000 when it gets there.
    handle.record(record(2)).into_result().unwrap();
    assert_eq!(
        readers.lock().unwrap().len(),
        2,
        "enqueueing a record does not read the stamp clock"
    );
    now.store(2_000, Ordering::SeqCst);
    gate_tx.send(()).unwrap();
    handle.sync().await.expect("sync past the gate");

    let landed = frames(&root);
    assert_eq!(landed.len(), 3);
    assert_eq!(landed[1].metadata.appended_ms, 1_000);
    assert_eq!(
        landed[2].metadata.appended_ms, 2_000,
        "the second frame carries the value read when the writer wrote it"
    );
    let readers = readers.lock().unwrap().clone();
    assert_eq!(readers, [writer_thread, writer_thread, writer_thread]);
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_frame_is_prepared_at_enqueue_and_the_floor_raised_before_it_is_written() {
    let root = Root::new("prepared", true);
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
    let handle = root.spawn(
        Box::new(GatedWal {
            inner: root.open(),
            reached: reached_tx,
            gate: Mutex::new(Some(gate_rx)),
        }),
        3,
        StampClock::wall(),
    );
    // An encode error surfaces at enqueue, on the caller's thread, because that is where the
    // version 4 payload is serialized. It is returned, never swallowed, and the writer fails
    // closed: nothing later can claim durability past the missing record.
    let refused = handle.record(WalRecord::Unknown).into_result();
    assert!(refused.is_err(), "{refused:?}");
    let _ = handle.record(record(1));
    drop(gate_tx);
    assert!(
        handle.sync().await.is_err(),
        "a refused encode poisons the writer, so no barrier can succeed after it"
    );
    assert!(
        reached_rx.try_recv().is_err(),
        "no record reached the log after the refusal"
    );
    assert_eq!(frames(&root).len(), 1, "only writer_opened was written");
    let _ = handle.shutdown().await;

    let root = Root::new("prepared-floor", true);
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
    let handle = root.spawn(
        Box::new(GatedWal {
            inner: root.open(),
            reached: reached_tx,
            gate: Mutex::new(Some(gate_rx)),
        }),
        3,
        StampClock::wall(),
    );
    handle.record(record(1)).into_result().unwrap();
    reached_rx
        .recv_timeout(gate_deadline())
        .expect("the writer reaches the gate");
    // The first application append is parked; only the writer-open barrier has landed.
    assert_eq!(frames(&root).len(), 1);
    gate_tx.send(()).unwrap();
    handle.sync().await.expect("sync past the gate");
    assert_eq!(frames(&root).len(), 2);
    assert_eq!(
        broca_wal::read_floor(&root.floor_path()).unwrap(),
        std::collections::BTreeSet::from(["frame-stamps/v1".to_owned(), "writer-id/v1".to_owned()]),
    );
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn appended_ms_monotonic_per_process() {
    let root = Root::new("monotonic", true);
    let handle = root.handle(3);
    for n in 1..=20 {
        handle.record(record(n)).into_result().unwrap();
        handle.sync().await.unwrap();
        let stamped = frames(&root);
        assert_eq!(stamped.len() as u64, n + 1);
        let mut last = 0;
        for header in &stamped {
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

#[tokio::test]
async fn crash_cut_inside_a_version_4_header_is_a_torn_tail() {
    let root = Root::new("crash-cut", true);
    let handle = root.handle(5);
    handle.record(record(1)).into_result().unwrap();
    handle.record(record(2)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");
    let complete = stored(&root);
    assert_eq!(frames(&root).len(), 4);

    // A crash mid-write leaves the first frame and its trailing identity frame whole, and cuts
    // the second frame's header partway, past the legacy header length recovery first reads.
    let first_app = second_frame_start(&complete);
    let second_start = first_app
        + broca_wal::framing::v4::decode_header(&complete[first_app..])
            .unwrap()
            .unwrap()
            .frame_len()
        + broca_wal::framing::LINEAGE_FRAME_LEN;
    let second = broca_wal::framing::v4::decode_header(&complete[second_start..])
        .unwrap()
        .expect("the second frame has a header");
    let cut = second_start + 64;
    assert!(cut < second_start + second.frame_len());
    std::fs::write(root.log_path(), &complete[..cut]).unwrap();

    let recovered = root.open();
    assert_eq!(
        recovered.last_seq(),
        2,
        "recovery discards the header that was cut short"
    );
    assert_eq!(
        frames(&root).len(),
        2,
        "only the open and first application frames survive"
    );
    // Appending after recovery continues from the surviving frame.
    let handle = root.spawn(recovered, 5, StampClock::wall());
    handle.record(record(3)).into_result().unwrap();
    handle.record(record(4)).into_result().unwrap();
    handle.shutdown().await.expect("shutdown");
    let landed = frames(&root);
    assert_eq!(landed.len(), 6);
    assert_eq!(landed[3].metadata.seq, 4);
    assert_eq!(landed[4].metadata.seq, 5);
}
