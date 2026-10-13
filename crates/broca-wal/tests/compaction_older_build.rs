//! Feature-forgetting is process-wide, so this binary contains exactly one test.
#![cfg(feature = "test-known-feature")]

use broca_wal::{framing, FileWal, WalError, WalRecord};

#[test]
fn a_wal_with_compaction_is_refused_not_skipped_by_an_older_build() {
    let root =
        broca_test_support::scratch_root().join(format!("compaction-older-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("s.wal");
    for json in include_str!("fixtures/compaction_records.jsonl").lines() {
        let record: WalRecord = serde_json::from_str(json).unwrap();
        let mut bytes = framing::encode(
            1,
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
        )
        .unwrap();
        bytes.extend(framing::encode(2, 1, &record).unwrap());
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(broca_wal::replay_file(&path, 1024 * 1024).unwrap().len(), 2);
        let feature = record.required_features()[0];
        let _older = broca_wal::forget_feature_for_test(feature);
        let error = broca_wal::replay_file(&path, 1024 * 1024).unwrap_err();
        assert!(
            matches!(error, WalError::NewerWriter { ref missing, .. } if missing == &[feature])
        );
        let error = match FileWal::open(&path) {
            Ok(_) => panic!("older writer must not open this WAL"),
            Err(e) => e,
        };
        assert!(
            matches!(error, WalError::NewerWriter { ref missing, .. } if missing == &[feature])
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "refusal must never repair/truncate intact history"
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
