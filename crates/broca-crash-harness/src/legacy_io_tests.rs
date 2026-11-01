use crate::io_trace::IoTraceFs;
use crate::power_loss::RecordingFs;
use broca_core::{session_addr, FileWalStore, ReadOnlyWalReader};
use broca_protocol::{BindIdentity, StepId};
use broca_wal::{fs_ops::with_fs_ops, FileWal, WalRecord};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

struct Fixture {
    root: PathBuf,
    path: PathBuf,
    bind: BindIdentity,
}
impl Fixture {
    fn new(label: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "legacy-io-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let bind = BindIdentity::new("/legacy-project", "harness", "legacy-io");
        let path = FileWalStore::new(root.join("wal")).wal_path(&bind);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            broca_wal::framing::encode(1, 1, &WalRecord::StepStarted { step_id: StepId(1) })
                .unwrap(),
        )
        .unwrap();
        Self { root, path, bind }
    }
    fn record(&self, work: impl FnOnce()) -> Vec<String> {
        let recording = Arc::new(RecordingFs::new(&self.root).unwrap());
        let fs = Arc::new(IoTraceFs::new(&self.root, recording.clone()));
        with_fs_ops(fs.clone(), work);
        assert!(!recording.operation_kinds().is_empty());
        fs.attempts()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn legacy_openers_have_exact_master_io() {
    let raw = Fixture::new("raw");
    let raw_trace = raw.record(|| {
        drop(FileWal::open(&raw.path).unwrap());
    });
    let reader = Fixture::new("reader");
    let reader_trace = reader.record(|| {
        assert_eq!(
            ReadOnlyWalReader::new(&reader.root)
                .records(&session_addr(&reader.bind))
                .unwrap()
                .0
                .len(),
            1
        );
    });
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/legacy-openers-master-io.json")).unwrap();
    assert_eq!(
        serde_json::to_value(vec![raw_trace, reader_trace]).unwrap(),
        serde_json::json!([expected["surfaces"][0], expected["surfaces"][1]]),
        "legacy filesystem attempts must equal the independently captured master trace"
    );
}

#[test]
fn missing_legacy_log_ignores_malformed_recovery_sidecar() {
    let missing = Fixture::new("missing");
    std::fs::remove_file(&missing.path).unwrap();
    let sidecar = PathBuf::from(format!("{}.cut-1.json", missing.path.display()));
    std::fs::write(&sidecar, b"malformed recovery manifest beside a legacy log").unwrap();
    let missing_trace = missing.record(|| {
        drop(
            FileWal::open(&missing.path)
                .expect("legacy creation must ignore unrelated recovery sidecars"),
        );
    });
    assert_eq!(
        std::fs::read(sidecar).unwrap(),
        b"malformed recovery manifest beside a legacy log"
    );
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/legacy-openers-master-io.json")).unwrap();
    assert_eq!(
        serde_json::to_value(missing_trace).unwrap(),
        expected["surfaces"][2],
        "legacy filesystem attempts must equal the independently captured master trace"
    );
}
