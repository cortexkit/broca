//! Power-loss cuts through the production writer-id/v1 lifecycle and FileWal recovery.

#[path = "../../broca-core/src/open_writers.rs"]
pub(super) mod actual_open_writers;

#[cfg(feature = "writer-id-faults")]
mod faults;
#[cfg(feature = "writer-id-faults")]
pub(crate) use faults::recovery_marker_sync_fault;
#[cfg(feature = "writer-id-faults")]
pub(crate) use faults::recovery_sync_fault;

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
    FileWal::open_with_recovery(
        root.join("wal/log"),
        broca_wal::WalConfig::OVERHAUL,
        root.join(broca_wal::FEATURE_FLOOR_FILENAME),
    )
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
        let (writer, seq, _, _) = WriterContext {
            root: root.0.clone(),
            address: broca_core::wal_writer::session_addr(&identity()),
            identity: identity(),
            cause: None,
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
            let (_, _, _) = writer.close(&mut wal, 1, 3).unwrap();
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
    PathBuf::from("open-writers").join(format!(
        "{}.{id}",
        broca_core::wal_writer::session_addr(&identity())
    ))
}

fn identity() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/writer-id", "harness", "power")
}

fn check_state(state: &State, root: &Scratch, writer_id: [u8; 16]) {
    root.clear();
    state.install(&root.0).unwrap();
    if let Some(bytes) = state.files.get(&marker(writer_id)) {
        assert_eq!(
            actual_open_writers::decode_writer_marker(bytes).unwrap(),
            identity()
        );
    }
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
    assert!(
        states.iter().any(|state| {
            !state.state.files.contains_key(&marker(id))
                && state.state.files.iter().any(|(path, bytes)| {
                    path.extension().is_some_and(|extension| extension == "tmp")
                        && actual_open_writers::decode_writer_marker(bytes).ok() == Some(identity())
                })
        }),
        "a cut after the temporary marker write but before rename must be exercised"
    );
    for state in states {
        check_state(&state.state, &recovery, id);
    }
}

#[test]
fn writer_id_power_loss_open_close_and_recovery_preserve_marker_or_closed() {
    let (_root, fs, id) = record_lifecycle(true);
    let first_recovery = Scratch::new();
    let second_recovery = Scratch::new();
    let states = fs.enumerate(100_000).unwrap();
    assert!(!states.is_empty());
    assert!(states
        .iter()
        .any(|state| state.family == Family::PartialPersistence));
    for (index, first) in states.into_iter().enumerate() {
        let root = first_recovery.0.join(format!("recovery-{index}"));
        first.state.install(&root).unwrap();
        let recovery = Arc::new(RecordingFs::new(&root).unwrap());
        with_fs_ops(recovery.clone(), || {
            let wal = open(&root);
            check_records(&wal.replay().unwrap(), root.join(marker(id)).exists(), id);
            wal.sync().unwrap();
        });
        // Recovery now publishes durable suffix backups as well as shortening
        // the log. Schedule every operation cut under the model's unchanged
        // per-cut budget instead of accumulating one budget across all recoveries.
        let mut covered = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for cut in 0..=recovery.operation_kinds().len() {
            let images = recovery.enumerate_cut(cut).unwrap();
            assert!(!images.is_empty());
            covered.push(images[0].clone());
            for image in images {
                // The marker invariant depends on the complete filesystem image,
                // not on which operation cut produced an identical image.
                if seen.insert(image.state.clone()) {
                    check_state(&image.state, &second_recovery, id);
                }
            }
        }
        recovery.assert_all_cuts_covered(&covered);
    }
}
