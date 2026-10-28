//! Power-loss cuts through durable-head publication and real FileWal recovery.

use crate::power_loss::{Family, RecordingFs, State};
use crate::writer_id_tests::actual_open_writers::WriterContext;
use broca_wal::{
    fs_ops::with_fs_ops,
    head_proof::{sidecar_path, HeadProof, PublishedLog, Publisher},
    FileWal, Wal, WalRecord,
};
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "head-proof-power-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn open(root: &Path) -> Result<FileWal, broca_wal::WalError> {
    FileWal::open(root.join("wal/log.wal"))?
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME))
        .with_wal_config(broca_wal::WalConfig::OVERHAUL)
}

fn publisher(wal: &FileWal) -> Publisher {
    Publisher::new(
        wal.log_path().unwrap().to_path_buf(),
        wal.lineage_id().unwrap(),
    )
}

fn writer_open(root: &Path, wal: &mut FileWal, now: u64) {
    let (writer, _, _) = WriterContext {
        root: root.to_path_buf(),
        address: "log".into(),
    }
    .open(wal, 1, || now)
    .unwrap();
    // Dropping the lifecycle object does not close a crashed writer or unlink its marker.
    drop(writer);
}

fn finish(wal: &mut FileWal) {
    let record = WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: None,
        reason: broca_protocol::caller::RunFinishReason::Completed,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    };
    wal.append_v4(
        1,
        &record,
        false,
        broca_wal::PreparedV4Frame::prepare(&record).unwrap(),
        1000,
    )
    .unwrap();
}

fn check_image(state: &State, root: &Scratch) -> bool {
    if root.0.exists() {
        std::fs::remove_dir_all(&root.0).unwrap();
    }
    state.install(&root.0).unwrap();
    let path = root.0.join("wal/log.wal");
    let bytes = std::fs::read(&path).unwrap_or_default();
    let sidecar = std::fs::read(sidecar_path(&path)).unwrap_or_default();
    if HeadProof::decode(&sidecar).is_some() {
        assert!(
            PublishedLog::open(&bytes, &sidecar).is_some(),
            "CRC-valid proof survived ahead of its synced frame"
        );
    }
    if let Some(published) = PublishedLog::open(&bytes, &sidecar) {
        let mut tail = &bytes[..published.head().durable_end as usize];
        let mut records = Vec::new();
        while !tail.is_empty() {
            match broca_wal::framing::decode_next(tail) {
                broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                    records.push(framed);
                    tail = &tail[consumed..];
                }
                broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                    tail = &tail[consumed..]
                }
                _ => panic!("validated published prefix failed record decoding"),
            }
        }
        if records.iter().any(|frame| {
            frame.seq <= published.head().durable_seq
                && matches!(frame.record, WalRecord::RunFinished { .. })
        }) {
            assert!(
                published.head().appended_ms >= 71_000,
                "old finish exposed without a newer frame: {:?}",
                published.head()
            );
            assert!(records.iter().any(|frame| matches!(frame.record, WalRecord::WriterOpened { recovered_through_seq, .. } if recovered_through_seq >= 2)));
            return true;
        }
    }
    false
}

#[test]
fn late_finish_never_exposed_without_newer_frame() {
    let mut total = 0;
    let mut exposed = 0;
    let mut old_proof_survived = false;
    let mut old_proof_missing = false;
    let mut new_unsynced_proof_survived = false;
    let mut refused_corrupt_tail = 0;
    for write_unsynced_proof in [false, true] {
        let root = Scratch::new();
        // Start from a real lineage file and record the writer-opened barrier.
        // Later cuts can lose the unsynced finish or sidecar independently.
        drop(open(&root.0).unwrap());
        let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
        with_fs_ops(recording.clone(), || {
            let mut wal = open(&root.0).unwrap();
            writer_open(&root.0, &mut wal, 1000);
            let mut head = publisher(&wal);
            head.sync(&wal).unwrap();
            if write_unsynced_proof {
                assert!(head.publish(1000).unwrap());
            }
            finish(&mut wal);
        });
        let recovery = Scratch::new();
        let check = Scratch::new();
        let states = recording
            .enumerate_recovery(&recovery.0, 2_000_000, |root| {
                let mut wal = match open(root) {
                    Ok(wal) => wal,
                    Err(broca_wal::WalError::Corrupt { .. }) => return Ok(()),
                    Err(error) => return Err(std::io::Error::other(error.to_string())),
                };
                let mut head = publisher(&wal);
                head.sync(&wal).unwrap();
                // A recovery sync alone must not expose the old finish. A second
                // power loss here leaves the old proof, a torn proof, or no proof.
                if wal.last_seq() > 0 {
                    assert!(
                        !head.publish(71_000).unwrap(),
                        "recovery sync published before writer_opened"
                    );
                }
                writer_open(root, &mut wal, 71_000);
                head.sync(&wal).unwrap();
                assert!(head.publish(71_000).unwrap());
                Ok(())
            })
            .unwrap();
        assert!(states
            .iter()
            .any(|state| state.first.family == Family::PartialPersistence));
        for state in states {
            total += 1;
            let old_head = state
                .first
                .state
                .files
                .get(&PathBuf::from("wal/log.wal.head"))
                .and_then(|bytes| HeadProof::decode(bytes));
            old_proof_survived |= old_head.is_some();
            old_proof_missing |= old_head.is_none();
            if check_image(&state.second.state, &check) {
                exposed += 1;
                new_unsynced_proof_survived = true;
            }
            if open(&check.0).is_err() {
                refused_corrupt_tail += 1;
            }
        }
    }
    assert!(old_proof_survived && old_proof_missing && new_unsynced_proof_survived);
    assert!(exposed > 0);
    eprintln!("two-crash publication: {total} H1 states; {exposed} fresh published heads; {refused_corrupt_tail} fail-closed corrupt tails");
}

#[test]
fn closed_writer_proof_covers_synced_close() {
    let root = Scratch::new();
    let mut wal = open(&root.0).unwrap();
    let (writer, _, _) = WriterContext {
        root: root.0.clone(),
        address: "closed".into(),
    }
    .open(&mut wal, 1, || 1000)
    .unwrap();
    let mut head = publisher(&wal);
    head.sync(&wal).unwrap();
    assert!(head.publish(1000).unwrap());
    let (closed_seq, _) = writer.close(&mut wal, 1, 2000).unwrap();
    head.sync(&wal).unwrap();
    assert!(head.publish(2000).unwrap());
    writer.unlink().unwrap();
    let path = wal.log_path().unwrap();
    let bytes = std::fs::read(path).unwrap();
    let sidecar = std::fs::read(sidecar_path(path)).unwrap();
    assert_eq!(
        PublishedLog::open(&bytes, &sidecar)
            .unwrap()
            .head()
            .durable_seq,
        closed_seq
    );
}
