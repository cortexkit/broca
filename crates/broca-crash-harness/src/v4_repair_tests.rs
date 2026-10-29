//! Crash cuts and a deliberately failed file barrier during writable repair.

use super::*;
#[cfg(feature = "writer-id-faults")]
use std::sync::atomic::Ordering;

fn damaged_image() -> (State, Vec<u8>) {
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
    assert_eq!(wal.last_seq(), 1);
    assert_eq!(
        wal.replay()
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect::<Vec<_>>(),
        [record(1)]
    );
    assert_eq!(std::fs::read(root.log()).unwrap(), expected);
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
        assert!(bytes == original || *bytes == repaired);
        if cut.cut > resize && cut.cut <= sync {
            pending_shrink |= bytes == original;
            persisted_shrink |= *bytes == repaired;
        }
        if cut.cut > sync {
            assert_eq!(
                *bytes, repaired,
                "the successful repair sync protects the shrink"
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
fn v4_recovery_failed_truncate_sync_is_not_transactional_and_retry_needs_a_barrier() {
    let (state, repaired) = damaged_image();
    let original = &state.files[&PathBuf::from("wal/log")];
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    let (fs, fail_sync, recording) =
        crate::writer_id_tests::recovery_sync_fault(&root.0, root.log());
    let before_barrier = with_fs_ops(fs, || {
        assert!(matches!(FileWal::open(root.log()), Err(WalError::Io(error))
            if error.to_string() == "injected writer close sync failure"));
        assert_eq!(
            std::fs::read(root.log()).unwrap(),
            repaired,
            "a failed sync does not roll the live truncate back"
        );
        assert!(!recording
            .operation_kinds()
            .contains(&OperationKind::SyncFile));

        fail_sync.store(false, Ordering::SeqCst);
        let retry_start = recording.operation_kinds().len();
        let mut retried = root.open();
        assert_eq!(retried.last_seq(), 1);

        let retry_ops = recording.operation_kinds();
        assert!(
            !retry_ops[retry_start..].contains(&OperationKind::SyncFile),
            "a clean shortened file does not trigger another repair sync on retry"
        );
        assert!(
            !retry_ops[retry_start..].contains(&OperationKind::Change(vec![ChangeKind::Resize]))
        );
        let retry_cut = retry_ops.len();
        let before_barrier: Vec<_> = recording
            .enumerate(100_000)
            .unwrap()
            .into_iter()
            .filter(|image| image.cut == retry_cut)
            .collect();

        retried.append(1, &record(99), true).unwrap();
        assert!(recording
            .operation_kinds()
            .contains(&OperationKind::SyncFile));
        before_barrier
    });
    assert!(before_barrier
        .iter()
        .any(|image| image.state.files[&PathBuf::from("wal/log")] == *original));
    assert!(before_barrier
        .iter()
        .any(|image| image.state.files[&PathBuf::from("wal/log")] == repaired));
    for image in before_barrier {
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
        assert_eq!(wal.last_seq(), 2);
        assert_eq!(wal.replay().unwrap().last().unwrap().record, record(99));
    }
}
