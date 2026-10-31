//! Crash cuts and a deliberately failed file barrier during writable repair.

use super::*;
#[cfg(feature = "writer-id-faults")]
use std::sync::atomic::Ordering;

pub(super) fn damaged_image() -> (State, Vec<u8>) {
    let (source, states) = images(2);
    let ends = boundaries(&source);
    let state = states
        .into_iter()
        .find(|s| {
            let data = &s.files[&PathBuf::from("wal/log")];
            data.get(ends[2].0..ends[4].0)
                .is_some_and(|bytes| bytes.iter().all(|b| *b == 0))
                && data.get(ends[4].0..ends[5].0) == Some(&source[ends[4].0..ends[5].0])
        })
        .expect("the model must leave damage before an intact later append");
    (state, source[..ends[2].0].to_vec())
}

fn assert_reopens(state: &State, expected: &[u8]) {
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let wal = root.open();
    let manifests = broca_wal::recovery::manifests(&root.log(), 1_000_000).unwrap();
    assert_eq!(
        manifests.len(),
        1,
        "an interrupted marker must reuse its original cut"
    );
    let cuts: Vec<_> = wal
        .replay()
        .unwrap()
        .into_iter()
        .filter_map(|frame| match frame.record {
            WalRecord::RecoveryCut { recovery } => Some(recovery.cut_id),
            _ => None,
        })
        .collect();
    assert_eq!(cuts, [manifests[0].recovery.cut_id.clone()]);
    let records: Vec<_> = wal
        .replay()
        .unwrap()
        .into_iter()
        .filter(|f| !matches!(f.record, WalRecord::RecoveryCut { .. }))
        .map(|f| f.record)
        .collect();
    assert_eq!(records, [record(1)]);
    assert!(std::fs::read(root.log()).unwrap().starts_with(expected));
    for (path, bytes) in &state.files {
        if path.to_string_lossy().contains(".cut-") {
            assert_eq!(
                std::fs::read(root.0.join(path)).unwrap(),
                *bytes,
                "recovery must never delete or overwrite preserved sidecars"
            );
        }
    }
}

#[test]
fn v4_recovery_torn_marker_reuses_the_original_cut_and_exact_bytes() {
    let (state, _) = damaged_image();
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    drop(root.open());
    let manifest = broca_wal::recovery::manifests(&root.log(), 1_000_000)
        .unwrap()
        .remove(0);
    let complete = std::fs::read(root.log()).unwrap();
    let start = manifest.recovery.cut_offset as usize;
    let saved = RecordingFs::new(&root.0)
        .unwrap()
        .enumerate(100)
        .unwrap()
        .remove(0)
        .state;
    for keep in [
        1,
        HEADER_LEN,
        (complete.len() - start) / 2,
        complete.len() - start - 1,
    ] {
        for zero_fill in [false, true] {
            let next = Scratch::new();
            saved.install(&next.0).unwrap();
            let mut damaged = complete[..start + keep].to_vec();
            if zero_fill {
                damaged.resize(complete.len(), 0);
            }
            std::fs::write(next.log(), damaged).unwrap();
            drop(next.open());
            let cuts = broca_wal::recovery::manifests(&next.log(), 1_000_000).unwrap();
            assert_eq!(cuts.len(), 1, "a torn marker is not a second cut");
            assert_eq!(cuts[0].recovery.cut_id, manifest.recovery.cut_id);
            assert_eq!(
                std::fs::read(next.log()).unwrap(),
                complete,
                "retry must write the precommitted exact marker"
            );
        }
    }
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn v4_recovery_refuse_finishes_manifest_directory_failure_before_truncation() {
    let (state, _) = damaged_image();
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let original = std::fs::read(root.log()).unwrap();
    let (fs, recording) =
        crate::writer_id_tests::recovery_manifest_dir_sync_fault(&root.0, root.log());
    let (images, retained_len) = with_fs_ops(fs, || {
        assert!(
            matches!(root.open_policy(broca_wal::recovery::RecoveryPolicy::Repair),
            Err(WalError::Io(error)) if error.to_string() == "injected manifest directory sync failure")
        );
        assert_eq!(std::fs::read(root.log()).unwrap(), original);
        let manifest = broca_wal::recovery::manifests(&root.log(), 1_000_000)
            .unwrap()
            .remove(0);
        let start = recording.operation_kinds().len();
        let wal = root
            .open_policy(broca_wal::recovery::RecoveryPolicy::Refuse)
            .unwrap();
        let ops = recording.operation_kinds();
        let resize = ops[start..]
            .iter()
            .position(|op| *op == OperationKind::Change(vec![ChangeKind::Resize]))
            .unwrap();
        assert!(
            ops[start..start + resize].contains(&OperationKind::SyncDirectory),
            "retry must repay the manifest directory barrier before truncation"
        );
        assert_eq!(
            broca_wal::recovery::manifests(&root.log(), 1_000_000)
                .unwrap()
                .len(),
            1
        );
        assert!(wal
            .replay()
            .unwrap()
            .iter()
            .any(|frame| matches!(&frame.record,
            WalRecord::RecoveryCut { recovery } if recovery.cut_id == manifest.recovery.cut_id)));
        let final_cut = recording.operation_kinds().len();
        let images = recording
            .enumerate(100_000)
            .unwrap()
            .into_iter()
            .filter(|image| image.cut == final_cut)
            .collect::<Vec<_>>();
        (images, manifest.recovery.cut_offset as usize)
    });
    assert!(!images.is_empty());
    for image in images {
        assert!(image
            .state
            .files
            .keys()
            .any(|p| p.to_string_lossy().ends_with(".json")));
        assert_reopens(&image.state, &original[..retained_len]);
    }
}

#[test]
fn v4_recovery_refuse_finishes_only_bound_marker_bytes_and_requires_evidence() {
    let (state, _) = damaged_image();
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    drop(root.open());
    let manifest = broca_wal::recovery::manifests(&root.log(), 1_000_000)
        .unwrap()
        .remove(0);
    let complete = std::fs::read(root.log()).unwrap();
    let start = manifest.recovery.cut_offset as usize;
    std::fs::write(root.log(), &complete[..start + HEADER_LEN]).unwrap();
    drop(
        root.open_policy(broca_wal::recovery::RecoveryPolicy::Refuse)
            .unwrap(),
    );
    assert_eq!(std::fs::read(root.log()).unwrap(), complete);

    let mut wal = root.open();
    wal.append(1, &record(99), true).unwrap();
    drop(wal);
    let mut damaged = std::fs::read(root.log()).unwrap();
    damaged[start + HEADER_LEN] ^= 1;
    std::fs::write(root.log(), &damaged).unwrap();
    let error = root
        .open_policy(broca_wal::recovery::RecoveryPolicy::Refuse)
        .err()
        .unwrap();
    assert!(matches!(error, WalError::Corrupt { .. }));
    assert_eq!(std::fs::read(root.log()).unwrap(), damaged);
    let raw = root
        .log()
        .parent()
        .unwrap()
        .join(&manifest.recovery.sidecar_basename);
    let path = PathBuf::from(format!("{}.json", raw.display()));
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    json.as_object_mut().unwrap().remove("marker");
    std::fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();
    let error = root
        .open_policy(broca_wal::recovery::RecoveryPolicy::Repair)
        .err()
        .unwrap();
    assert!(error.to_string().contains(&path.display().to_string()));
    assert!(error.to_string().contains("marker"));
    assert_eq!(std::fs::read(root.log()).unwrap(), damaged);
}

#[test]
fn v4_recovery_every_repair_cut_including_between_truncate_and_sync_reopens() {
    let (state, repaired) = damaged_image();
    let original = &state.files[&PathBuf::from("wal/log")];
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
    with_fs_ops(recording.clone(), || drop(root.open()));
    let operations = recording.operation_kinds();
    let resize = operations
        .iter()
        .position(|op| *op == OperationKind::Change(vec![ChangeKind::Resize]))
        .unwrap();
    let sync = operations
        .iter()
        .enumerate()
        .skip(resize + 1)
        .find(|(_, op)| **op == OperationKind::SyncFile)
        .unwrap()
        .0;
    let cuts = recording.enumerate(100_000).unwrap();
    recording.assert_all_cuts_covered(&cuts);
    let mut pending_shrink = false;
    let mut persisted_shrink = false;
    for cut in cuts {
        let bytes = &cut.state.files[&PathBuf::from("wal/log")];
        assert!(bytes.starts_with(&repaired));
        if cut.cut > resize && cut.cut <= sync {
            pending_shrink |= bytes == original;
            persisted_shrink |= *bytes == repaired;
        }
        if cut.cut > sync {
            assert!(bytes.starts_with(&repaired));
        }
        if bytes != original {
            let manifest = cut
                .state
                .files
                .iter()
                .filter(|(path, _)| path.to_string_lossy().ends_with(".json"))
                .filter_map(|(_, bytes)| {
                    serde_json::from_slice::<broca_wal::recovery::CutManifest>(bytes).ok()
                })
                .find(|manifest| manifest.recovery.cut_offset == repaired.len() as u64)
                .expect("a persisted truncate requires a complete durable manifest first");
            assert_eq!(
                cut.state.files[&PathBuf::from("wal").join(manifest.recovery.sidecar_basename)],
                original[repaired.len()..]
            );
        }
        assert_reopens(&cut.state, &repaired);
    }
    assert!(
        pending_shrink && persisted_shrink,
        "cuts before repair sync must exercise both lost and surviving truncation"
    );
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn v4_recovery_failed_truncate_sync_retries_from_durable_manifest() {
    let (state, repaired) = damaged_image();
    let original = &state.files[&PathBuf::from("wal/log")];
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let (fs, fail_sync, recording) =
        crate::writer_id_tests::recovery_sync_fault(&root.0, root.log());
    let failed_states = with_fs_ops(fs, || {
        assert!(
            matches!(FileWal::open_with_recovery(root.log(), WalConfig::OVERHAUL, root.0.join("floor")), Err(WalError::Io(error))
            if error.to_string() == "injected writer close sync failure")
        );
        assert_eq!(
            std::fs::read(root.log()).unwrap(),
            repaired,
            "a failed sync does not roll the live truncate back"
        );
        let failed_cut = recording.operation_kinds().len();
        let failed_states: Vec<_> = recording
            .enumerate(100_000)
            .unwrap()
            .into_iter()
            .filter(|s| s.cut == failed_cut)
            .collect();
        fail_sync.store(false, Ordering::SeqCst);
        let retry_start = recording.operation_kinds().len();
        let mut retried = root.open();
        let records = retried.replay().unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|f| matches!(f.record, WalRecord::RecoveryCut { .. }))
                .count(),
            1
        );
        let retry_ops = recording.operation_kinds();
        assert!(
            retry_ops[retry_start..].contains(&OperationKind::SyncFile),
            "retry must durably finish the pending recovery marker"
        );
        assert!(
            !retry_ops[retry_start..].contains(&OperationKind::Change(vec![ChangeKind::Resize]))
        );
        retried.append(1, &record(99), true).unwrap();
        failed_states
    });
    assert!(failed_states
        .iter()
        .any(|s| s.state.files[&PathBuf::from("wal/log")] == *original));
    assert!(failed_states
        .iter()
        .any(|s| s.state.files[&PathBuf::from("wal/log")] == repaired));
    for image in failed_states {
        assert_reopens(&image.state, &repaired);
    }
    let final_cut = recording.operation_kinds().len();
    let synced = std::fs::read(root.log()).unwrap();
    for image in recording
        .enumerate(100_000)
        .unwrap()
        .into_iter()
        .filter(|s| s.cut == final_cut)
    {
        assert_eq!(image.state.files[&PathBuf::from("wal/log")], synced);
        let next = Scratch::new();
        image.state.install(&next.0).unwrap();
        let wal = next.open();
        assert_eq!(wal.replay().unwrap().last().unwrap().record, record(99));
    }
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn v4_recovery_marker_sync_failure_retries_the_visible_marker_barrier() {
    let (state, retained) = damaged_image();
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let (fs, recording) = crate::writer_id_tests::recovery_marker_sync_fault(&root.0, root.log());
    with_fs_ops(fs, || {
        assert!(
            matches!(FileWal::open_with_recovery(root.log(), WalConfig::OVERHAUL, root.0.join("floor")), Err(WalError::Io(error))
            if error.to_string() == "injected recovery marker sync failure")
        );
        let visible = std::fs::read(root.log()).unwrap();
        assert!(visible.starts_with(&retained));
        let retry_start = recording.operation_kinds().len();
        let wal = root.open();
        assert_eq!(std::fs::read(root.log()).unwrap(), visible);
        assert_eq!(
            wal.replay()
                .unwrap()
                .iter()
                .filter(|f| matches!(f.record, WalRecord::RecoveryCut { .. }))
                .count(),
            1
        );
        assert!(
            recording.operation_kinds()[retry_start..].contains(&OperationKind::SyncFile),
            "a complete visible marker still needs a successful retry barrier"
        );
        let after_retry = recording.operation_kinds().len();
        let images = recording.enumerate(100_000).unwrap();
        assert!(images.iter().any(|image| image.cut == after_retry));
        for image in images.into_iter().filter(|image| image.cut == after_retry) {
            assert_eq!(image.state.files[&PathBuf::from("wal/log")], visible);
        }
    });
}
