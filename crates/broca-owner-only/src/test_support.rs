//! Native Windows fixtures. PowerShell reads ACLs independently of the production validator.
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

fn powershell(path: &Path, script: &str) -> String {
    let quoted = path.to_string_lossy().replace('\'', "''");
    let output = Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            &format!("$ErrorActionPreference='Stop'; $p='{quoted}'; {script}"),
        ])
        .output()
        .expect("Windows ACL fixture requires PowerShell");
    assert!(
        output.status.success(),
        "ACL fixture failed for {}: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

pub fn acl_snapshot(path: &Path) -> String {
    powershell(path, "(Get-Acl -LiteralPath $p).Sddl")
}

pub fn assert_private(path: &Path) {
    powershell(
        path,
        r#"
        $acl=Get-Acl -LiteralPath $p;
        $owner=[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value;
        $entries=@($acl.Access);
        if ($entries.Count -ne 2) { throw 'expected exactly account and SYSTEM ACEs' };
        $ownerFound=$false;
        foreach ($ace in $entries) {
            $sid=$ace.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).Value;
            if ($ace.AccessControlType -ne 'Allow' -or ($sid -ne $owner -and $sid -ne 'S-1-5-18')) { throw 'non-private principal' };
            if ($sid -eq $owner -and $ace.FileSystemRights -eq 'FullControl') { $ownerFound=$true };
        };
        if (-not $ownerFound) { throw 'account lacks full control' };
    "#,
    );
}

pub fn assert_protected_private(path: &Path) {
    assert_private(path);
    powershell(path, "if (-not (Get-Acl -LiteralPath $p).AreAccessRulesProtected) { throw 'creation DACL is not protected' }");
}

pub fn assert_inherited_private(path: &Path) {
    assert_private(path);
    powershell(path, "if (@((Get-Acl -LiteralPath $p).Access | Where-Object { -not $_.IsInherited }).Count -ne 0) { throw 'sidecar was not inherited' }");
}

pub fn broaden(path: &Path) {
    powershell(
        path,
        r#"
        $acl=Get-Acl -LiteralPath $p;
        $sid=New-Object System.Security.Principal.SecurityIdentifier('S-1-1-0');
        if (Test-Path -LiteralPath $p -PathType Container) {
            $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow');
        } else { $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($sid,'FullControl','Allow') };
        $acl.AddAccessRule($rule); Set-Acl -LiteralPath $p -AclObject $acl;
    "#,
    );
}

pub fn set_owner_to_administrators(path: &Path) {
    powershell(path, "$acl=Get-Acl -LiteralPath $p; $acl.SetOwner((New-Object System.Security.Principal.SecurityIdentifier('S-1-5-32-544'))); Set-Acl -LiteralPath $p -AclObject $acl");
}

/// Makes the file's ACL unreadable to its owner. Windows grants an owner READ_CONTROL
/// implicitly, whatever the DACL says, unless an OWNER RIGHTS (S-1-3-4) entry is present; a
/// deny entry on the owner's own SID alone therefore changes nothing for the owner.
pub fn deny_acl_read(path: &Path) {
    powershell(
        path,
        r#"
        $acl=Get-Acl -LiteralPath $p;
        $ownerRights=New-Object System.Security.Principal.SecurityIdentifier('S-1-3-4');
        $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($ownerRights,'ReadPermissions','Deny');
        $acl.AddAccessRule($rule); Set-Acl -LiteralPath $p -AclObject $acl;
    "#,
    );
}

pub fn reset_acl(path: &Path) {
    let output = Command::new("icacls.exe")
        .arg(path)
        .arg("/reset")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "reset ACL: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub struct BroadParent {
    pub path: PathBuf,
}
impl BroadParent {
    pub fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "broca-acl-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        super::create_dir(&path).unwrap();
        broaden(&path);
        let control = path.join("inherited-control");
        std::fs::write(&control, b"observable inheritance").unwrap();
        assert!(
            acl_snapshot(&control).contains("WD"),
            "Everyone inheritance control is absent"
        );
        assert!(
            super::validate(&control).is_err(),
            "broad inherited ACL was accepted"
        );
        Self { path }
    }
}
impl Drop for BroadParent {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

type Observer = Box<dyn FnMut(&Path)>;
thread_local! { static OBSERVER: RefCell<Option<Observer>> = const { RefCell::new(None) }; }
#[cfg(feature = "test-support")]
pub(crate) fn created(path: &Path) {
    OBSERVER.with(|observer| {
        if let Some(observer) = observer.borrow_mut().as_mut() {
            observer(path);
        }
    });
}
/// Inspect temporary files while their handles are open, before publication or cleanup.
pub fn observe<T>(observer: impl FnMut(&Path) + 'static, run: impl FnOnce() -> T) -> T {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVER.with(|observer| *observer.borrow_mut() = None);
        }
    }
    OBSERVER.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(observer));
    });
    let _reset = Reset;
    run()
}
