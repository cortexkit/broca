use broca_wal::{archive::ArchiveWriter, FileWal, Wal, WalRecord};
use std::process::Command;

#[test]
fn wal_verify_exits_zero_against_prefold_baseline_on_half_folded_tree() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-archive-inventory-{}", std::process::id()));
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let cold = root.join("wal/0123456789abcdef.wal");
    let warm = root.join("wal/fedcba9876543210.wal");
    for path in [&cold, &warm] {
        let mut wal = FileWal::open(path).unwrap();
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
    }
    let baseline = root.join("baseline");
    let output = Command::new(env!("CARGO_BIN_EXE_wal-verify"))
        .arg("baseline")
        .arg(&root)
        .arg(&baseline)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(&cold).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(cold).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_wal-verify"))
        .arg("check")
        .arg(&root)
        .arg(&baseline)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("all 2 baseline WAL identities present")
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// Write a WAL whose second frame needs a feature no build knows yet, as a newer build would.
fn newer_writer_wal(path: &std::path::Path) {
    let record = WalRecord::StepStarted {
        step_id: broca_protocol::StepId(1),
    };
    FileWal::open(path)
        .unwrap()
        .append(1, &record, true)
        .unwrap();
    let frame =
        broca_wal::framing::encode_with_requires(2, 1, &["from-the-future/v1"], &record).unwrap();
    let mut bytes = std::fs::read(path).unwrap();
    bytes.extend_from_slice(&frame);
    std::fs::write(path, bytes).unwrap();
}

/// `wal-verify` reports a WAL that a newer build wrote as `newer-writer` (NewerWriter), not as
/// damaged, and still exits 2, because it cannot count the frames of a WAL it cannot read.
/// This holds for a live WAL and for an archived one.
#[test]
fn wal_verify_reports_a_newer_writer_wal_by_name_and_fails() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-verify-newer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let live = root.join("wal/0123456789abcdef.wal");
    newer_writer_wal(&live);

    let output = Command::new(env!("CARGO_BIN_EXE_wal-verify"))
        .arg("baseline")
        .arg(&root)
        .arg(root.join("baseline"))
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.contains("newer-writer 0123456789abcdef.wal"),
        "{stderr}"
    );
    assert!(stderr.contains("from-the-future/v1"), "{stderr}");
    assert!(!stderr.contains("damaged"), "{stderr}");

    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(&live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(&live).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_wal-verify"))
        .arg("baseline")
        .arg(&root)
        .arg(root.join("baseline"))
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("NewerWriter"), "{stderr}");
    std::fs::remove_dir_all(root).unwrap();
}

/// `wal-verify` reads a WAL whose `RunStarted` froze a scope (a gated frame requiring
/// `scope/v1`, which this build knows) as a healthy WAL, live and archived: `baseline` and
/// `check` both succeed and count it, rather than reporting it as a newer writer.
#[test]
fn wal_verify_reads_a_scoped_wal_live_and_archived() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-verify-scoped-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let live = root.join("wal/0123456789abcdef.wal");
    let scoped: WalRecord = serde_json::from_str(
        r#"{"type":"run_started","run_id":"run-1","session":{"project_root":"/p","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"},"scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3}}"#,
    )
    .unwrap();
    FileWal::open(&live)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME))
        .append(1, &scoped, true)
        .unwrap();
    let verify = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_wal-verify"))
            .arg(command)
            .arg(&root)
            .arg(root.join("baseline"))
            .output()
            .unwrap()
    };
    let output = verify("baseline");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("newer-writer"), "{stderr}");

    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(&live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(&live).unwrap();
    let output = verify("check");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("all 1 baseline WAL identities present"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// `wal-verify` reads a WAL whose `RunStarted` froze a plan manifest (a gated frame requiring
/// `plan-manifest/v1`, which this build knows) as a healthy WAL, live and archived: `baseline`
/// and `check` both succeed and count it, rather than reporting it as a newer writer.
#[test]
fn wal_verify_reads_a_manifest_wal_live_and_archived() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-verify-manifest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let live = root.join("wal/0123456789abcdef.wal");
    let manifest: WalRecord = serde_json::from_str(
        &include_str!("../fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "run-1"),
    )
    .unwrap();
    FileWal::open(&live)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME))
        .append(1, &manifest, true)
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap(),
        "plan-manifest/v1\n"
    );
    let verify = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_wal-verify"))
            .arg(command)
            .arg(&root)
            .arg(root.join("baseline"))
            .output()
            .unwrap()
    };
    let output = verify("baseline");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("newer-writer"), "{stderr}");

    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(&live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(&live).unwrap();
    let output = verify("check");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("all 1 baseline WAL identities present"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// `wal-verify` reads a WAL holding a steered message (a gated frame requiring
/// `steer-queue/v1`, which this build knows) as a healthy WAL, live and from an archive member.
#[test]
fn wal_verify_reads_a_steered_wal_live_and_archived() {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-verify-steered-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let live = root.join("wal/0123456789abcdef.wal");
    let started: WalRecord = serde_json::from_str(
        r#"{"type":"run_started","run_id":"run-1","session":{"project_root":"/p","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}}"#,
    )
    .unwrap();
    let steered: WalRecord = serde_json::from_str(
        r#"{"type":"steered","send_id":"steer-1","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
    )
    .unwrap();
    let mut wal = FileWal::open(&live)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    wal.append(1, &started, true).unwrap();
    wal.append(1, &steered, true).unwrap();
    drop(wal);
    assert_eq!(
        std::fs::read_to_string(root.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap(),
        "steer-queue/v1\n"
    );
    let verify = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_wal-verify"))
            .arg(command)
            .arg(&root)
            .arg(root.join("baseline"))
            .output()
            .unwrap()
    };
    let output = verify("baseline");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("newer-writer"), "{stderr}");

    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(&live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(&live).unwrap();
    let output = verify("check");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("all 1 baseline WAL identities present"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// `wal-verify` reads a WAL holding a tool dispatch intent that names its module (a gated frame
/// requiring `dispatch-module/v1`, which this build knows) as a healthy WAL, live and from an
/// archive member, and the file reader gives the module back.
#[test]
fn wal_verify_reads_a_dispatch_module_wal_live_and_archived() {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-verify-dispatch-module-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let live = root.join("wal/0123456789abcdef.wal");
    let intent: WalRecord = serde_json::from_str(
        r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"},"module_id":"aft","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2}}"#,
    )
    .unwrap();
    let mut wal = FileWal::open(&live)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    wal.append(
        1,
        &WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    wal.append(1, &intent, true).unwrap();
    drop(wal);
    assert_eq!(
        std::fs::read_to_string(root.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap(),
        "dispatch-module/v1\n"
    );
    let records = broca_wal::replay_file(&live, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    assert_eq!(records.last().map(|framed| &framed.record), Some(&intent));
    let verify = |command: &str| {
        Command::new(env!("CARGO_BIN_EXE_wal-verify"))
            .arg(command)
            .arg(&root)
            .arg(root.join("baseline"))
            .output()
            .unwrap()
    };
    let output = verify("baseline");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(!stderr.contains("newer-writer"), "{stderr}");

    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(&live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(&live).unwrap();
    let output = verify("check");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("all 1 baseline WAL identities present"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    std::fs::remove_dir_all(root).unwrap();
}
