//! Creation-time permissions for files containing prompts, credentials or durable state.
#![deny(unsafe_code)]

use std::{fs::File, io, path::Path};

#[cfg(windows)]
#[allow(unsafe_code)]
mod windows;

#[cfg(all(windows, any(test, feature = "test-support")))]
pub mod test_support;

/// The subset of file-open options used by private state writers.
#[derive(Default)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    append: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn read(&mut self, value: bool) -> &mut Self {
        self.read = value;
        self
    }
    pub fn write(&mut self, value: bool) -> &mut Self {
        self.write = value;
        self
    }
    pub fn append(&mut self, value: bool) -> &mut Self {
        self.append = value;
        self
    }
    pub fn create(&mut self, value: bool) -> &mut Self {
        self.create = value;
        self
    }
    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.create_new = value;
        self
    }
    pub fn truncate(&mut self, value: bool) -> &mut Self {
        self.truncate = value;
        self
    }

    /// Windows supplies a protected owner+SYSTEM DACL to the creating syscall. Existing
    /// files are checked on the opened handle before any truncation or caller IO.
    pub fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        #[cfg(windows)]
        {
            let file = windows::open_file(path.as_ref(), self)?;
            #[cfg(feature = "test-support")]
            test_support::created(path.as_ref());
            Ok(file)
        }
        #[cfg(not(windows))]
        {
            let mut options = std::fs::OpenOptions::new();
            options
                .read(self.read)
                .write(self.write)
                .append(self.append)
                .create(self.create)
                .create_new(self.create_new)
                .truncate(self.truncate);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options.open(path)
        }
    }
}

pub fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
    create_directory(path.as_ref(), false)
}

pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    create_directory(path.as_ref(), true)
}

fn create_directory(path: &Path, recursive: bool) -> io::Result<()> {
    #[cfg(windows)]
    {
        windows::create_directory(path, recursive)
    }
    #[cfg(not(windows))]
    {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(recursive);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path)
    }
}

/// Reject unknown or non-private Windows ACLs. Other platforms retain their callers'
/// existing mode-validation policy, rather than acquiring extra open-time restrictions.
pub fn validate(path: impl AsRef<Path>) -> io::Result<()> {
    #[cfg(windows)]
    {
        windows::validate(path.as_ref())
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(windows)]
pub fn validate_inheritable_directory(path: impl AsRef<Path>) -> io::Result<()> {
    windows::validate_inheritable_directory(path.as_ref())
}

pub fn validate_if_exists(path: impl AsRef<Path>) -> io::Result<()> {
    match validate(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// Distinguish an inspectable insecure ACL (false) from an unreadable ACL (an error).
#[cfg(windows)]
pub fn is_private(path: impl AsRef<Path>) -> io::Result<bool> {
    windows::is_private(path.as_ref())
}

/// Repair only this object, preserving its owner. Reparse points are refused, never followed.
#[cfg(windows)]
pub fn repair(path: impl AsRef<Path>) -> io::Result<()> {
    windows::repair(path.as_ref())
}

/// True for every Windows reparse point, including junctions which are not Rust symlinks.
pub fn is_reparse(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

#[cfg(all(test, unix))]
mod unix_tests {
    use super::*;
    use std::{
        io::Write,
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "broca-private-unix-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }
    #[test]
    fn unix_new_objects_are_owner_only() {
        let fixture = Fixture::new();
        let dir = fixture.0.join("nested/leaf");
        create_dir_all(&dir).unwrap();
        let file = dir.join("file");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file)
            .unwrap();
        assert_eq!(mode(&file), 0o600);
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&fixture.0.join("nested")), 0o700);
        assert_eq!(
            mode(&fixture.0),
            0o755,
            "shared ancestor must not be narrowed"
        );
    }
    #[test]
    fn unix_existing_modes_are_not_repaired_on_open() {
        let fixture = Fixture::new();
        let file = fixture.0.join("file");
        std::fs::write(&file, b"old").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file)
            .unwrap()
            .write_all(b"new")
            .unwrap();
        create_dir_all(&fixture.0).unwrap();
        assert_eq!(mode(&file), 0o644);
        assert_eq!(mode(&fixture.0), 0o755);
        assert_eq!(std::fs::read(&file).unwrap(), b"oldnew");
    }
    #[test]
    fn unix_open_options_keep_exclusive_and_truncate_semantics() {
        let fixture = Fixture::new();
        let file = fixture.0.join("file");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file)
            .unwrap()
            .write_all(b"original")
            .unwrap();
        assert_eq!(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file)
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&file)
            .unwrap()
            .write_all(b"new")
            .unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"new");
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::io::Write;
    use test_support::{assert_protected_private, broaden, reset_acl, BroadParent};
    #[test]
    fn windows_creation_ignores_broad_parent_acl() {
        let fixture = BroadParent::new("helper");
        let dir = fixture.path.join("nested/leaf");
        create_dir_all(&dir).unwrap();
        let path = fixture.path.join("file");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap()
            .write_all(b"private")
            .unwrap();
        for candidate in [&dir, &fixture.path.join("nested"), &path] {
            assert_protected_private(candidate);
        }
    }
    #[test]
    fn windows_file_open_refuses_before_truncating() {
        let fixture = BroadParent::new("truncate");
        let path = fixture.path.join("file");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap()
            .write_all(b"unchanged")
            .unwrap();
        broaden(&path);
        assert!(OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)
            .is_err());
        reset_acl(&path);
        assert_eq!(std::fs::read(&path).unwrap(), b"unchanged");
    }
}
