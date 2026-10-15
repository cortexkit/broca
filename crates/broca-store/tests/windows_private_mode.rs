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
fn windows_private_mode_observes_unreadable_acl() {
    let fixture = BroadParent::new("repair-unreadable");
    let (root, _) = tree(&fixture.path);
    let file = root.join("wal/session.wal");
    deny_acl_read(&file);
    // A privileged token can read the ACL despite the deny. This test records whether
    // ACL inspection actually returns PermissionDenied on Windows; the portable
    // injected test verifies that private-mode refuses when inspection is denied.
    match broca_owner_only::is_private(&file) {
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            observation(format_args!(
                "observation: ACL inspection really returned PermissionDenied"
            ));
        }
        inspection => {
            observation(format_args!(
                "observation: cannot construct an unreadable ACL: {inspection:?}"
            ));
            observe_acl_environment(&file);
            reset_acl(&file);
            assert_eq!(std::fs::read(&file).unwrap(), b"WAL marker");
            return;
        }
    }
    for dry_run in [true, false] {
        let result = run(&root, dry_run);
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("cannot inspect ACL"));
    }
    reset_acl(&file);
    assert_eq!(std::fs::read(&file).unwrap(), b"WAL marker");
}

fn observation(message: std::fmt::Arguments<'_>) {
    // Write directly so a passing observation remains visible under libtest capture.
    use std::io::Write;
    writeln!(std::io::stderr().lock(), "{message}").unwrap();
}

fn observe_acl_environment(file: &Path) {
    let privileges = Command::new("whoami.exe").arg("/priv").output().unwrap();
    observation(format_args!(
        "whoami /priv ({}):\n{}{}",
        privileges.status,
        String::from_utf8_lossy(&privileges.stdout),
        String::from_utf8_lossy(&privileges.stderr)
    ));
    let script = r#"
$ErrorActionPreference = 'Stop'
Add-Type @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class AclTokenObservation {
    [StructLayout(LayoutKind.Sequential)]
    public struct Luid { public uint Low; public int High; }
    [StructLayout(LayoutKind.Sequential)]
    public struct PrivilegeSet {
        public uint Count, Control;
        public Luid Luid;
        public uint Attributes;
    }
    [DllImport("advapi32.dll", SetLastError = true)]
    static extern bool GetTokenInformation(IntPtr token, int kind, out int value, int size, out int returned);
    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern bool LookupPrivilegeValue(string system, string name, out Luid luid);
    [DllImport("advapi32.dll", SetLastError = true)]
    static extern bool PrivilegeCheck(IntPtr token, ref PrivilegeSet required, out bool enabled);
    public static int Elevation(IntPtr token) {
        int value, returned;
        if (!GetTokenInformation(token, 20, out value, sizeof(int), out returned))
            throw new Win32Exception();
        return value;
    }
    public static bool BackupEnabled(IntPtr token) {
        var required = new PrivilegeSet { Count = 1, Control = 1, Attributes = 2 };
        if (!LookupPrivilegeValue(null, "SeBackupPrivilege", out required.Luid))
            throw new Win32Exception();
        bool enabled;
        if (!PrivilegeCheck(token, ref required, out enabled))
            throw new Win32Exception();
        return enabled;
    }
}
'@
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
Write-Output ('TokenIsElevated={0}' -f [AclTokenObservation]::Elevation($identity.Token))
Write-Output ('SeBackupPrivilegeEnabled={0}' -f [AclTokenObservation]::BackupEnabled($identity.Token))
Write-Output 'File SDDL:'
try { Get-Acl -LiteralPath $env:BROCA_ACL_OBSERVATION_PATH | % Sddl }
catch { Write-Output ('Get-Acl failed: {0}' -f $_) }
"#;
    let diagnostics = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("BROCA_ACL_OBSERVATION_PATH", file)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&diagnostics.stdout);
    observation(format_args!(
        "token and ACL observation ({}):\n{stdout}{}",
        diagnostics.status,
        String::from_utf8_lossy(&diagnostics.stderr)
    ));
    if stdout.contains("SeBackupPrivilegeEnabled=True") {
        observation(format_args!(
            "observation: enabled SeBackupPrivilege means this environment cannot construct \
             the unreadable-ACL state with a DACL deny"
        ));
    } else {
        observation(format_args!(
            "observation: the unreadable-ACL state was not reached; \
             SeBackupPrivilege was not observed enabled (see diagnostics above)"
        ));
    }
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
    let result = Command::new("cmd.exe")
        .args(["/C", "mklink", "/J"])
        .arg(&root_link)
        .arg(&root)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "root junction fixture: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(!run(&root_link, false).status.success());
    std::fs::remove_dir(root_link).unwrap();
    std::fs::remove_dir(link).unwrap();
}
