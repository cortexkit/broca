use super::*;
use crate::{FileWal, Wal, WalRecord};
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Io {
    CreateDir,
    Open,
    Lock,
    Read,
    FileSync,
    Rename,
    DirSync,
}

#[derive(Default)]
struct Trace {
    events: Vec<Io>,
    cut: Option<Io>,
    crash: bool,
}

thread_local! {
    static TRACE: RefCell<Option<Trace>> = const { RefCell::new(None) };
}

pub(super) fn observe(event: Io) -> std::io::Result<()> {
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace {
            trace.events.push(event);
            if trace.cut == Some(event) {
                if trace.crash {
                    std::process::exit(86);
                }
                return Err(std::io::Error::other(format!("floor crash cut: {event:?}")));
            }
        }
        Ok(())
    })
}

fn traced<T>(cut: Option<Io>, action: impl FnOnce() -> T) -> (T, Vec<Io>) {
    struct Restore(Option<Trace>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TRACE.with_borrow_mut(|trace| *trace = self.0.take());
        }
    }
    let _restore = Restore(TRACE.replace(Some(Trace {
        events: Vec::new(),
        cut,
        crash: false,
    })));
    let result = action();
    let events = TRACE.with_borrow_mut(|trace| std::mem::take(&mut trace.as_mut().unwrap().events));
    (result, events)
}

fn gated_record() -> WalRecord {
    serde_json::from_value(serde_json::json!({
        "type": "run_cancel_requested", "run_id": "floor-memo", "at": "now"
    }))
    .unwrap()
}

fn covered_io() -> Vec<Io> {
    vec![
        Io::CreateDir,
        Io::Open,
        Io::Lock,
        Io::Read,
        Io::Open,
        Io::FileSync,
        Io::DirSync,
    ]
}

#[test]
fn covered_gated_append_has_no_floor_io() {
    let root = super::tests::scratch("memo-hot");
    let floor = root.join(FEATURE_FLOOR_FILENAME);
    let mut wal = FileWal::open(root.join("writer.wal"))
        .unwrap()
        .with_feature_floor(&floor);
    let record = gated_record();
    let (result, first) = traced(None, || wal.append(1, &record, true));
    result.unwrap();
    assert_eq!(
        first,
        vec![
            Io::CreateDir,
            Io::Open,
            Io::Lock,
            Io::Read,
            Io::Open,
            Io::FileSync,
            Io::Rename,
            Io::DirSync
        ]
    );
    let (result, second) = traced(None, || wal.append(1, &record, true));
    result.unwrap();
    assert!(
        second.is_empty(),
        "covered append performed floor I/O: {second:?}"
    );
    let mut other = FileWal::open(root.join("other.wal"))
        .unwrap()
        .with_feature_floor(&floor);
    let (result, shared) = traced(None, || other.append(1, &record, true));
    result.unwrap();
    assert!(
        shared.is_empty(),
        "the memo must be process-wide, not writer-local"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn cold_process_syncs_a_covered_floor_once() {
    const CHILD_ROOT: &str = "BROCA_FLOOR_COLD_CHILD_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        let mut wal = FileWal::open(root.join("child.wal"))
            .unwrap()
            .with_feature_floor(root.join(FEATURE_FLOOR_FILENAME));
        let record = gated_record();
        let (result, events) = traced(None, || wal.append(1, &record, true));
        result.unwrap();
        assert_eq!(
            events,
            covered_io(),
            "a readable floor is not yet this process's durability proof"
        );
        let (result, events) = traced(None, || wal.append(1, &record, true));
        result.unwrap();
        assert!(
            events.is_empty(),
            "only the first covered append syncs the floor"
        );
        return;
    }
    let root = super::tests::scratch("memo-cold-process");
    raise_floor(
        &root.join(FEATURE_FLOOR_FILENAME),
        gated_record().required_features(),
    )
    .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "floor::io_tests::cold_process_syncs_a_covered_floor_once",
            "--nocapture",
        ])
        .env(CHILD_ROOT, &root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn new_names_and_distinct_floors_still_raise() {
    let root = super::tests::scratch("memo-new-name");
    let floor = root.join(FEATURE_FLOOR_FILENAME);
    raise_floor(&floor, &["scope/v1"]).unwrap();
    let (result, events) = traced(None, || {
        raise_floor(&floor, &["scope/v1", "session-class/v1"])
    });
    result.unwrap();
    assert!(events.contains(&Io::Rename));
    assert!(events.contains(&Io::DirSync));
    assert_eq!(
        read_floor(&floor).unwrap(),
        BTreeSet::from(["scope/v1".to_owned(), "session-class/v1".to_owned()])
    );
    let (result, events) = traced(None, || {
        raise_floor(&floor, &["session-class/v1", "scope/v1"])
    });
    result.unwrap();
    assert!(events.is_empty());
    let other = root.join("other-floor");
    let (result, events) = traced(None, || raise_floor(&other, &["scope/v1"]));
    result.unwrap();
    assert!(events.contains(&Io::FileSync));
    assert_eq!(
        read_floor(&other).unwrap(),
        BTreeSet::from(["scope/v1".to_owned()])
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_floor_sync_never_memoizes_or_writes_a_gated_frame() {
    for cut in [Io::FileSync, Io::Rename, Io::DirSync] {
        let root = super::tests::scratch(&format!("memo-failed-{cut:?}"));
        let path = root.join("writer.wal");
        let floor = root.join(FEATURE_FLOOR_FILENAME);
        let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        let before = std::fs::read(&path).unwrap();
        for _ in 0..2 {
            let (result, events) = traced(Some(cut), || wal.append(1, &gated_record(), true));
            assert!(
                matches!(result, Err(crate::WalError::FeatureFloor { .. })),
                "cut {cut:?}: {result:?}"
            );
            assert!(
                events.contains(&cut),
                "retry skipped the failed durability step"
            );
            wal.sync().unwrap();
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before,
                "floor cut {cut:?} left frame bytes"
            );
            assert!(wal.replay().unwrap().is_empty());
        }
        wal.append(1, &gated_record(), true).unwrap();
        assert_eq!(wal.replay().unwrap().len(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn concurrent_covered_raises_share_one_durability_proof() {
    let root = super::tests::scratch("memo-concurrent");
    let floor = root.join(FEATURE_FLOOR_FILENAME);
    std::fs::write(&floor, "scope/v1\n").unwrap();
    let barrier = std::sync::Barrier::new(8);
    let events: Vec<_> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    let (result, events) = traced(None, || raise_floor(&floor, &["scope/v1"]));
                    result.unwrap();
                    events
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    });
    assert_eq!(events, covered_io());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn crash_cuts_never_publish_a_frame_before_its_floor() {
    const CHILD_ROOT: &str = "BROCA_FLOOR_CRASH_CHILD_ROOT";
    const CHILD_CUT: &str = "BROCA_FLOOR_CRASH_CHILD_CUT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        let cut = match std::env::var(CHILD_CUT).unwrap().as_str() {
            "FileSync" => Io::FileSync,
            "Rename" => Io::Rename,
            "DirSync" => Io::DirSync,
            other => panic!("unknown crash cut {other}"),
        };
        let mut wal = FileWal::open(root.join("writer.wal"))
            .unwrap()
            .with_feature_floor(root.join(FEATURE_FLOOR_FILENAME));
        TRACE.replace(Some(Trace {
            events: Vec::new(),
            cut: Some(cut),
            crash: true,
        }));
        wal.append(1, &gated_record(), true).unwrap();
        panic!("the floor crash cut was not reached");
    }
    // Exit the writer at each publication boundary, without unwinding or cleaning up its
    // temporary file. Recovery must see only the earlier, synced ungated frame, even when
    // the new floor was visible just before the process died at its directory barrier.
    for cut in [Io::FileSync, Io::Rename, Io::DirSync] {
        let root = super::tests::scratch(&format!("memo-crash-{cut:?}"));
        let path = root.join("writer.wal");
        let floor = root.join(FEATURE_FLOOR_FILENAME);
        let plain: WalRecord = serde_json::from_value(serde_json::json!({
            "type": "step_started", "step_id": 1
        }))
        .unwrap();
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &plain, true).unwrap();
        drop(wal);
        let before = std::fs::read(&path).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "floor::io_tests::crash_cuts_never_publish_a_frame_before_its_floor",
                "--nocapture",
            ])
            .env(CHILD_ROOT, &root)
            .env(CHILD_CUT, format!("{cut:?}"))
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(86),
            "cut {cut:?} not reached: {output:?}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "cut {cut:?} published a gated frame"
        );
        let mut recovered = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        assert_eq!(recovered.replay().unwrap().len(), 1);
        recovered.append(1, &gated_record(), true).unwrap();
        assert_eq!(
            read_floor(&floor).unwrap(),
            gated_record()
                .required_features()
                .iter()
                .map(|name| (*name).to_owned())
                .collect()
        );
        assert_eq!(recovered.replay().unwrap().len(), 2);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[ignore = "synthetic 10,000-frame timing measurement; run explicitly with --ignored --nocapture"]
fn gated_append_10000_measurement() {
    let root = super::tests::scratch("measurement");
    let floor = root.join(FEATURE_FLOOR_FILENAME);
    let mut wal = FileWal::open(root.join("writer.wal"))
        .unwrap()
        .with_feature_floor(&floor);
    let record = gated_record();
    assert!(!record.required_features().is_empty());
    let start = std::time::Instant::now();
    let (_, events) = traced(None, || {
        for _ in 0..10_000 {
            wal.append(1, &record, true).unwrap();
        }
    });
    let elapsed = start.elapsed();
    eprintln!(
        "10,000 gated sync=true appends: {elapsed:?}, {:.3} us/append; floor opens={}, locks={}, reads={}, file_syncs={}, dir_syncs={}",
        elapsed.as_secs_f64() * 1_000_000.0 / 10_000.0,
        events.iter().filter(|&&op| op == Io::Open).count(),
        events.iter().filter(|&&op| op == Io::Lock).count(),
        events.iter().filter(|&&op| op == Io::Read).count(),
        events.iter().filter(|&&op| op == Io::FileSync).count(),
        events.iter().filter(|&&op| op == Io::DirSync).count(),
    );
    assert_eq!(wal.replay().unwrap().len(), 10_000);
    std::fs::remove_dir_all(root).unwrap();
}
