#![cfg(windows)]
use broca_owner_only::test_support::{
    acl_snapshot, assert_private, deny_acl_read, reset_acl, set_owner_to_administrators,
    BroadParent,
};
use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};
fn run(root: &Path, dry_run: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_store-maintenance"));
    command.arg("private-mode").arg(root);
    if dry_run {
        command.arg("--dry-run");
    }
    command.output().unwrap()
}
fn tree(parent: &Path) -> (PathBuf, Vec<PathBuf>) {
    let root = parent.join("state");
    std::fs::create_dir(&root).unwrap();
    let wal = root.join("wal");
    std::fs::create_dir(&wal).unwrap();
    let db = root.join("run-index.db");
    std::fs::write(&db, b"database marker").unwrap();
    let file = wal.join("session.wal");
    std::fs::write(&file, b"WAL marker").unwrap();
    (root.clone(), vec![root, wal, db, file])
}
#[test]
fn windows_private_mode_dry_run_and_idempotent_repair() {
    let fixture = BroadParent::new("repair");
    let (root, paths) = tree(&fixture.path);
    // Windows CI runs as an administrator; a group-owned file distinguishes owner
    // preservation from silently replacing every owner with the current account.
    set_owner_to_administrators(&root.join("run-index.db"));
    let owners: Vec<_> = paths
        .iter()
        .map(|p| acl_snapshot(p).split("D:").next().unwrap().to_owned())
        .collect();
    let original: Vec<_> = paths.iter().map(|p| acl_snapshot(p)).collect();
    let ancestor = acl_snapshot(&fixture.path);
    assert!(run(&root, true).status.success());
    assert_eq!(
        paths.iter().map(|p| acl_snapshot(p)).collect::<Vec<_>>(),
        original
    );
    let repaired = run(&root, false);
    assert!(
        repaired.status.success(),
        "{}",
        String::from_utf8_lossy(&repaired.stderr)
    );
    for path in &paths {
        assert_private(path);
    }
    assert_eq!(
        paths
            .iter()
            .map(|p| acl_snapshot(p).split("D:").next().unwrap().to_owned())
            .collect::<Vec<_>>(),
        owners,
        "repair changed an owner"
    );
    let after: Vec<_> = paths.iter().map(|p| acl_snapshot(p)).collect();
    let second = run(&root, false);
    assert!(second.status.success());
    assert!(String::from_utf8_lossy(&second.stdout).contains("tightened 0"));
    assert_eq!(
        paths.iter().map(|p| acl_snapshot(p)).collect::<Vec<_>>(),
        after
    );
    assert_eq!(acl_snapshot(&fixture.path), ancestor);
    assert_eq!(
        std::fs::read(root.join("run-index.db")).unwrap(),
        b"database marker"
    );
    assert_eq!(
        std::fs::read(root.join("wal/session.wal")).unwrap(),
        b"WAL marker"
    );
}
#[test]
fn windows_private_mode_refuses_unreadable_acl() {
    let fixture = BroadParent::new("repair-unreadable");
    let (root, _) = tree(&fixture.path);
    let file = root.join("wal/session.wal");
    deny_acl_read(&file);
    for dry_run in [true, false] {
        let result = run(&root, dry_run);
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("cannot inspect ACL"));
    }
    reset_acl(&file);
    assert_eq!(std::fs::read(&file).unwrap(), b"WAL marker");
}
#[test]
fn windows_private_mode_never_follows_junctions() {
    let fixture = BroadParent::new("repair-junction");
    let (root, _) = tree(&fixture.path);
    let external = fixture.path.join("external");
    std::fs::create_dir(&external).unwrap();
    let target = external.join("untouched");
    std::fs::write(&target, b"external marker").unwrap();
    let link = root.join("junction");
    let result = Command::new("cmd.exe")
        .args(["/C", "mklink", "/J"])
        .arg(&link)
        .arg(&external)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "junction fixture: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let snapshots = [
        acl_snapshot(&fixture.path),
        acl_snapshot(&external),
        acl_snapshot(&target),
    ];
    let result = run(&root, false);
    assert!(
        !result.status.success(),
        "skipping an unrepairable junction must not report a complete repair"
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("refusing reparse point"));
    assert_eq!(
        [
            acl_snapshot(&fixture.path),
            acl_snapshot(&external),
            acl_snapshot(&target)
        ],
        snapshots
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"external marker");
    // A supplied junction root is refused too, not just a link discovered during descent.
    let root_link = fixture.path.join("root-link");
    assert!(Command::new("cmd.exe")
        .args(["/C", "mklink", "/J"])
        .arg(&root_link)
        .arg(&root)
        .status()
        .unwrap()
        .success());
    assert!(!run(&root_link, false).status.success());
    std::fs::remove_dir(root_link).unwrap();
    std::fs::remove_dir(link).unwrap();
}
