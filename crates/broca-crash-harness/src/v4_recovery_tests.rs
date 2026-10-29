//! Power-loss recovery at a published version 4 durability boundary.

#[path = "v4_repair_tests.rs"]
mod repair;

use crate::power_loss::{ChangeKind, OperationKind, RecordingFs, State};
use broca_protocol::StepId;
use broca_wal::{
    framing::{decode_next, DecodeStep, HEADER_LEN, LINEAGE_FRAME_LEN},
    fs_ops::with_fs_ops,
    head_proof::{sidecar_path, HeadProof, Publisher, SyncedPrefix},
    FileWal, Wal, WalConfig, WalError, WalRecord,
};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "v4-recovery-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn log(&self) -> PathBuf {
        self.0.join("wal/log")
    }

    fn open(&self) -> FileWal {
        FileWal::open(self.log())
            .unwrap()
            .with_feature_floor(self.0.join("floor"))
            .with_wal_config(WalConfig::OVERHAUL)
            .unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn record(n: u64) -> WalRecord {
    WalRecord::StepStarted { step_id: StepId(n) }
}

fn published(root: &Scratch) -> FileWal {
    let mut wal = root.open();
    wal.append(1, &record(1), true).unwrap();
    let mut publisher = Publisher::from_synced(SyncedPrefix::sync(&wal).unwrap()).unwrap();
    publisher.publish(|| 0).unwrap();
    wal
}

fn images(pending: u64) -> (Vec<u8>, Vec<State>) {
    let root = Scratch::new();
    drop(published(&root));
    let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
    with_fs_ops(recording.clone(), || {
        let mut wal = root.open();
        for n in 2..=pending + 1 {
            wal.append(1, &record(n), false).unwrap();
        }
    });
    let source = std::fs::read(root.log()).unwrap();
    let states = recording.enumerate(100_000_000).unwrap();
    recording.assert_all_cuts_covered(&states);
    let states = states
        .into_iter()
        .map(|image| image.state)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    (source, states)
}

// The oracle uses the original bytes and frame boundaries, never the damaged
// image's decoder. An intact pending prefix can have synced before publication.
fn boundaries(source: &[u8]) -> Vec<(usize, usize)> {
    let mut offset = 0;
    let mut records = 0;
    let mut ends = Vec::new();
    while offset < source.len() {
        match decode_next(&source[offset..]) {
            DecodeStep::Record { consumed, .. } => {
                records += 1;
                offset += consumed;
            }
            DecodeStep::Lineage { consumed, .. } => offset += consumed,
            _ => panic!("source is a complete writer-produced log"),
        }
        ends.push((offset, records));
    }
    ends
}

fn assert_recovery(source: &[u8], state: &State) {
    let data = &state.files[&PathBuf::from("wal/log")];
    let (end, count) = boundaries(source)
        .into_iter()
        .take_while(|(end, _)| data.get(..*end) == Some(&source[..*end]))
        .last()
        .unwrap();
    assert!(count >= 1, "the synced frame must survive every image");
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
    with_fs_ops(recording.clone(), || {
        let mut wal = root.open();
        assert_eq!(std::fs::read(root.log()).unwrap(), source[..end]);
        if data.len() > end {
            let operations = recording.operation_kinds();
            let resize = operations
                .iter()
                .position(|op| *op == OperationKind::Change(vec![ChangeKind::Resize]))
                .expect("repair must truncate through the recording backend");
            assert!(
                operations[resize + 1..].contains(&OperationKind::SyncFile),
                "repair must sync before open returns, not rely on the next append's sync"
            );
        }
        assert_eq!(wal.last_seq(), count as u64);
        let recovered: Vec<_> = wal
            .replay()
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect();
        assert_eq!(
            recovered,
            (1..=count as u64).map(record).collect::<Vec<_>>()
        );
        assert_eq!(wal.append(1, &record(99), true).unwrap(), count as u64 + 1);
    });
    let final_cut = recording.operation_kinds().len();
    for second in recording
        .enumerate(100_000)
        .unwrap()
        .into_iter()
        .filter(|s| s.cut == final_cut)
    {
        assert_eq!(
            second.state.files[&PathBuf::from("wal/log")],
            std::fs::read(root.log()).unwrap(),
            "repair and the new append must both be durable"
        );
        let reopened = Scratch::new();
        second.state.install(&reopened.0).unwrap();
        let wal = reopened.open();
        assert_eq!(wal.last_seq(), count as u64 + 1);
        assert_eq!(wal.replay().unwrap().last().unwrap().record, record(99));
    }
}

#[test]
fn v4_recovery_all_power_loss_images_with_one_to_three_pending_appends() {
    for pending in 1..=3 {
        let (source, states) = images(pending);
        assert!(states.len() > 1);
        eprintln!(
            "{pending} pending appends: {} distinct cut images",
            states.len()
        );
        for state in states {
            assert_recovery(&source, &state);
        }
    }
}

#[test]
fn v4_recovery_zero_filled_unsynced_tail() {
    let (source, states) = images(2);
    let ends = boundaries(&source);
    let state = states
        .iter()
        .find(|s| {
            let data = &s.files[&PathBuf::from("wal/log")];
            data.get(ends[2].0..ends[4].0)
                .is_some_and(|b| b.iter().all(|b| *b == 0))
                && data.get(ends[4].0..ends[5].0) == Some(&source[ends[4].0..ends[5].0])
        })
        .expect("the harness must persist length and a later frame without early pending data");
    assert_recovery(&source, state);
}

#[test]
fn v4_recovery_torn_unsynced_tail_before_intact_later_frame() {
    let (source, states) = images(2);
    let ends = boundaries(&source);
    let pending_start = ends[2].0;
    let later_start = ends[4].0;
    let state = states
        .iter()
        .find(|s| {
            let data = &s.files[&PathBuf::from("wal/log")];
            data.get(pending_start..pending_start + HEADER_LEN)
                == Some(&source[pending_start..pending_start + HEADER_LEN])
                && data
                    .get(pending_start + HEADER_LEN..later_start)
                    .is_some_and(|b| b.iter().all(|b| *b == 0))
                && data.get(later_start..ends[5].0) == Some(&source[later_start..ends[5].0])
        })
        .expect(
            "the harness must persist a torn early frame and an intact later frame out of order",
        );
    assert_recovery(&source, state);
}

#[test]
fn v4_recovery_refuses_damage_inside_proof() {
    let root = Scratch::new();
    drop(published(&root));
    let proof = HeadProof::decode(&std::fs::read(sidecar_path(&root.log())).unwrap()).unwrap();
    let source = std::fs::read(root.log()).unwrap();
    for byte in [
        proof.durable_end as usize - 1,
        LINEAGE_FRAME_LEN + 5,
        LINEAGE_FRAME_LEN + 4,
    ] {
        let mut data = source.clone();
        data[byte] ^= 1;
        std::fs::write(root.log(), &data).unwrap();
        assert!(
            matches!(FileWal::open(root.log()), Err(WalError::Corrupt { .. })),
            "covered byte {byte}"
        );
        assert_eq!(std::fs::read(root.log()).unwrap(), data);
    }
}

#[test]
fn v4_recovery_refuses_conflicting_lineage_after_damage() {
    let root = Scratch::new();
    let mut wal = published(&root);
    let start = std::fs::metadata(root.log()).unwrap().len() as usize;
    wal.append(1, &record(2), false).unwrap();
    wal.append(1, &record(3), false).unwrap();
    drop(wal);
    let foreign = Scratch::new();
    drop(published(&foreign));
    let foreign_lineage = &std::fs::read(foreign.log()).unwrap()[..LINEAGE_FRAME_LEN];
    let mut damaged = std::fs::read(root.log()).unwrap();
    let pending_end = boundaries(&damaged)[3].0;
    damaged[pending_end - 1] ^= 1;
    damaged[pending_end..pending_end + LINEAGE_FRAME_LEN].copy_from_slice(foreign_lineage);
    for image in [&damaged[..pending_end + LINEAGE_FRAME_LEN], &damaged[..]] {
        std::fs::write(root.log(), image).unwrap();
        assert!(
            matches!(
                FileWal::open(root.log()),
                Err(WalError::ConflictingLineageIds { .. })
            ),
            "foreign lineage following damage at {start} must not be discarded"
        );
        assert_eq!(std::fs::read(root.log()).unwrap(), image);
    }
}

#[test]
fn v4_recovery_combined_lineage_and_covered_damage_uses_final_tail_fallback() {
    let root = Scratch::new();
    drop(published(&root));
    let proof_bytes = std::fs::read(sidecar_path(&root.log())).unwrap();
    let proof = HeadProof::decode(&proof_bytes).unwrap();
    let original = std::fs::read(root.log()).unwrap();
    let mut lost_lineage = original.clone();
    lost_lineage[HEADER_LEN] ^= 1;
    std::fs::write(root.log(), &lost_lineage).unwrap();
    assert!(matches!(
        FileWal::open(root.log()),
        Err(WalError::Corrupt { .. })
    ));
    assert_eq!(std::fs::read(root.log()).unwrap(), lost_lineage);

    // Without an intact record after the damaged lineage frame, the final-tail
    // fallback cannot distinguish lost appends from corruption of synced data.
    lost_lineage[proof.durable_end as usize - 1] ^= 1;
    std::fs::write(root.log(), &lost_lineage).unwrap();
    let reopened = FileWal::open(root.log()).unwrap();
    assert_eq!(reopened.last_seq(), 0);
    assert!(reopened.replay().unwrap().is_empty());
    assert!(std::fs::read(root.log()).unwrap().is_empty());
    assert_eq!(
        std::fs::read(sidecar_path(&root.log())).unwrap(),
        proof_bytes
    );
}

#[test]
fn v4_recovery_missing_or_invalid_proof_keeps_strict_refusal() {
    let (source, states) = images(2);
    let ends = boundaries(&source);
    let state = states
        .iter()
        .find(|s| {
            let data = &s.files[&PathBuf::from("wal/log")];
            data.get(ends[2].0..ends[4].0)
                .is_some_and(|b| b.iter().all(|b| *b == 0))
                && data.get(ends[4].0..ends[5].0) == Some(&source[ends[4].0..ends[5].0])
        })
        .unwrap();
    for variant in 0..11 {
        let root = Scratch::new();
        state.install(&root.0).unwrap();
        let sidecar = sidecar_path(&root.log());
        let mut proof = HeadProof::decode(&std::fs::read(&sidecar).unwrap()).unwrap();
        match variant {
            0 => std::fs::remove_file(&sidecar).unwrap(),
            1 => std::fs::write(&sidecar, b"torn proof").unwrap(),
            2 => {
                proof.lineage_id[0] ^= 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            3 => {
                proof.durable_end = source.len() as u64 + 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            4 => {
                proof.frame_identity[0] ^= 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            5 => {
                proof.durable_seq += 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            6 => {
                std::fs::remove_file(&sidecar).unwrap();
                std::fs::create_dir(&sidecar).unwrap();
            }
            7 => {
                proof.appended_ms ^= 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            8 => {
                proof.writer_id[0] ^= 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            9 => {
                proof.durable_end -= 1;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            10 => {
                proof.durable_end += LINEAGE_FRAME_LEN as u64;
                std::fs::write(&sidecar, proof.encode()).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(FileWal::open(root.log()), Err(WalError::Corrupt { .. })),
            "proof variant {variant}"
        );
        assert_eq!(
            std::fs::read(root.log()).unwrap(),
            state.files[&PathBuf::from("wal/log")]
        );
    }
}

#[test]
fn v4_recovery_disables_proof_policy_after_a_legacy_record() {
    let root = Scratch::new();
    drop(published(&root));
    let mut legacy = FileWal::open(root.log()).unwrap();
    legacy.append(1, &record(2), false).unwrap();
    let damaged_start = std::fs::metadata(root.log()).unwrap().len() as usize;
    legacy.append(1, &record(3), false).unwrap();
    legacy.append(1, &record(4), false).unwrap();
    drop(legacy);
    let mut data = std::fs::read(root.log()).unwrap();
    data[damaged_start + HEADER_LEN] ^= 1;
    std::fs::write(root.log(), &data).unwrap();
    assert!(matches!(
        FileWal::open(root.log()),
        Err(WalError::Corrupt { .. })
    ));
    assert_eq!(std::fs::read(root.log()).unwrap(), data);
}

#[test]
fn v4_recovery_leaves_pinned_legacy_bytes_and_tail_policy_unchanged() {
    let hex = include_str!("fixtures/legacy-step-started.hex").trim();
    let fixture: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    for tail in [&[][..], &[0; 30][..]] {
        let root = Scratch::new();
        std::fs::create_dir_all(root.log().parent().unwrap()).unwrap();
        let mut image = fixture.clone();
        image.extend_from_slice(tail);
        std::fs::write(root.log(), image).unwrap();
        let wal = FileWal::open(root.log()).unwrap();
        assert_eq!(wal.wal_config(), WalConfig::LEGACY);
        assert_eq!(wal.last_seq(), 1);
        assert_eq!(wal.replay().unwrap()[0].record, record(1));
        assert_eq!(std::fs::read(root.log()).unwrap(), fixture);
    }
}
