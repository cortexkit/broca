//! Power-loss cuts through the production writer-id/v1 lifecycle and FileWal recovery.

#[path = "../../broca-core/src/open_writers.rs"]
mod actual_open_writers;

use crate::power_loss::{Family, RecordingFs, State};
use actual_open_writers::WriterContext;
use broca_wal::{fs_ops::with_fs_ops, FileWal, Wal, WalRecord};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "writer-id-power-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }
    fn clear(&self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(root: &Path) -> FileWal {
    FileWal::open(root.join("wal/log"))
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME))
        .with_wal_config(broca_wal::WalConfig::OVERHAUL)
        .unwrap()
}

fn record_lifecycle(close: bool) -> (Scratch, Arc<RecordingFs>, [u8; 16]) {
    let root = Scratch::new();
    let fs = Arc::new(RecordingFs::new(&root.0).unwrap());
    let writer_id = with_fs_ops(fs.clone(), || {
        let mut wal = open(&root.0);
        let id = wal.writer_id().unwrap();
        let (writer, seq, _) = WriterContext {
            root: root.0.clone(),
            address: "addr".into(),
        }
        .open(&mut wal, 1, || 1)
        .unwrap();
        assert_eq!(seq, 1);
        if close {
            let record = WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            };
            let prepared = broca_wal::PreparedV4Frame::prepare(&record).unwrap();
            wal.append_v4(1, &record, false, prepared, 2).unwrap();
            wal.sync().unwrap();
            writer.close(&mut wal, 1, 3).unwrap();
            writer.unlink().unwrap();
        }
        id
    });
    (root, fs, writer_id)
}

fn check_records(records: &[broca_wal::FramedRecord], marker_exists: bool, writer_id: [u8; 16]) {
    let opened = records.iter().any(|frame| matches!(frame.record, WalRecord::WriterOpened { writer_id: id, .. } if id == writer_id));
    let closed = records.iter().any(|frame| matches!(frame.record, WalRecord::WriterClosed { writer_id: id } if id == writer_id));
    assert!(
        !opened || marker_exists || closed,
        "writer_opened survived without its marker or writer_closed"
    );
}

fn marker(writer_id: [u8; 16]) -> PathBuf {
    let id: String = writer_id.iter().map(|byte| format!("{byte:02x}")).collect();
    PathBuf::from("open-writers").join(format!("addr.{id}"))
}

fn check_state(state: &State, root: &Scratch, writer_id: [u8; 16]) {
    root.clear();
    state.install(&root.0).unwrap();
    let wal = open(&root.0);
    check_records(
        &wal.replay().unwrap(),
        state.files.contains_key(&marker(writer_id)),
        writer_id,
    );
}

#[test]
fn marker_durable_before_writer_opened() {
    let (_root, fs, id) = record_lifecycle(false);
    let recovery = Scratch::new();
    let states = fs.enumerate(100_000).unwrap();
    assert!(states
        .iter()
        .any(|state| state.family == Family::PartialPersistence));
    assert!(states
        .iter()
        .any(|state| state.state.files.contains_key(&marker(id))));
    for state in states {
        check_state(&state.state, &recovery, id);
    }
}

#[test]
fn writer_id_power_loss_open_close_and_recovery_preserve_marker_or_closed() {
    let (_root, fs, id) = record_lifecycle(true);
    let first_recovery = Scratch::new();
    let second_recovery = Scratch::new();
    let states = fs
        .enumerate_recovery(&first_recovery.0, 100_000, |root| {
            let wal = open(root);
            check_records(&wal.replay().unwrap(), root.join(marker(id)).exists(), id);
            wal.sync()
                .map_err(|error| std::io::Error::other(error.to_string()))
        })
        .unwrap();
    assert!(!states.is_empty());
    assert!(states
        .iter()
        .any(|state| state.first.family == Family::PartialPersistence));
    for state in states {
        check_state(&state.second.state, &second_recovery, id);
    }
}
