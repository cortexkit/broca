use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use futures::executor::block_on;

use super::{
    prune_versions, BackendError, LocalBackend, SessionAddr, SessionBackend, HEAD_VERSION_KEEP_MS,
};
use crate::codec::hex;

fn root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "broca-backend-{}",
        hex(&crate::keys::random_id().0)
    ))
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn versions_dir(root: &std::path::Path) -> PathBuf {
    root.join("heads").join(&addr().0).join("v")
}

/// Sets the time version `version`'s file was written to `age` before now.
fn written_ago(root: &std::path::Path, version: u64, age: Duration) {
    std::fs::File::options()
        .write(true)
        .open(versions_dir(root).join(version.to_string()))
        .unwrap()
        .set_modified(SystemTime::now() - age)
        .unwrap();
}

/// Every head write also leaves that version as its own file, read back byte for byte, while
/// `head` holds only the newest.
#[test]
fn every_head_written_is_kept_as_its_own_version() {
    let root = root();
    let backend = LocalBackend::new(&root);
    block_on(async {
        backend.write_head(&addr(), b"one", None, 1).await.unwrap();
        backend
            .write_head(&addr(), b"two", Some(1), 2)
            .await
            .unwrap();
        backend
            .write_head(&addr(), b"five", Some(2), 5)
            .await
            .unwrap();
        assert_eq!(backend.head_versions(&addr()).unwrap(), [1, 2, 5]);
        assert_eq!(
            backend.read_head_version(&addr(), 1).unwrap().unwrap(),
            b"one"
        );
        assert_eq!(
            backend.read_head_version(&addr(), 2).unwrap().unwrap(),
            b"two"
        );
        assert_eq!(backend.read_head_version(&addr(), 3).unwrap(), None);
        assert_eq!(
            backend.read_head(&addr()).await.unwrap(),
            Some((b"five".to_vec(), 5))
        );
        // A refused write leaves no version behind.
        assert!(matches!(
            backend.write_head(&addr(), b"late", Some(2), 3).await,
            Err(BackendError::Fenced)
        ));
        assert_eq!(backend.head_versions(&addr()).unwrap(), [1, 2, 5]);
    });
    // A version file holding another version than its name says is damage.
    std::fs::copy(versions_dir(&root).join("1"), versions_dir(&root).join("4")).unwrap();
    assert!(matches!(
        backend.read_head_version(&addr(), 4),
        Err(BackendError::Corrupt(_))
    ));
    std::fs::remove_dir_all(&root).unwrap();
}

/// A version goes once the version after it has existed for a day; younger ones stay, and the
/// newest two stay however old they are.
#[test]
fn a_version_goes_a_day_after_the_next_and_the_newest_two_stay() {
    let root = root();
    let backend = LocalBackend::new(&root);
    let day = Duration::from_millis(HEAD_VERSION_KEEP_MS);
    let hour = Duration::from_secs(3600);
    block_on(async {
        for version in 1..=4 {
            let expected = (version > 1).then(|| version - 1);
            backend
                .write_head(&addr(), b"h", expected, version)
                .await
                .unwrap();
        }
        // Version 2 was written over a day ago, so version 1 is due; version 3 is an hour old,
        // so version 2 isn't yet.
        written_ago(&root, 2, day + hour);
        written_ago(&root, 3, hour);
        backend.write_head(&addr(), b"h", Some(4), 5).await.unwrap();
        assert_eq!(backend.head_versions(&addr()).unwrap(), [2, 3, 4, 5]);
        // A version whose next one is a day old is due.
        for version in [3, 4, 5] {
            written_ago(&root, version, day);
        }
        backend.write_head(&addr(), b"h", Some(5), 6).await.unwrap();
        assert_eq!(backend.head_versions(&addr()).unwrap(), [5, 6]);
    });
    // However old, the newest two stay.
    let years = SystemTime::now() + 1000 * day;
    assert_eq!(prune_versions(&versions_dir(&root), years).unwrap(), 0);
    assert_eq!(backend.head_versions(&addr()).unwrap(), [5, 6]);
    std::fs::remove_dir_all(&root).unwrap();
}

#[cfg(unix)]
fn assert_mode(path: &std::path::Path, expected: u32) {
    use std::os::unix::fs::PermissionsExt;

    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        expected,
        "unexpected mode for {}",
        path.display()
    );
}

#[cfg(unix)]
#[test]
fn local_backend_creates_owner_only_files_and_folders() {
    use std::os::unix::fs::PermissionsExt;

    let parent = root();
    std::fs::create_dir_all(&parent).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    // A move stages a session in its own backend root, with the same layout as a live store.
    for relative in ["session-store", "session-store-moves/staging/move"] {
        let root = parent.join(relative);
        let backend = LocalBackend::new(&root);
        let log = crate::codec::Id128([7; 16]);
        let object = format!("{}/pages/00", hex(&log.0));
        block_on(async {
            backend.write_head(&addr(), b"one", None, 1).await.unwrap();
            backend
                .write_head(&addr(), b"two", Some(1), 2)
                .await
                .unwrap();
            backend.create_log(&addr(), &log, 2).await.unwrap();
            backend
                .append_log(&addr(), &log, b"log tail", 0, 2, true)
                .await
                .unwrap();
            backend
                .put_object(&addr(), &object, b"object", 2)
                .await
                .unwrap();
            backend.put_setup("aa", b"encrypted setup").await.unwrap();
            backend.put_setup("plain/bb", b"plain setup").await.unwrap();
        });
        let cut = backend.cut_log(&addr(), &log, 3, 123).unwrap();
        assert_eq!(std::fs::read(&cut).unwrap(), b" tail");
        let head = backend.head_dir(&addr());
        let objects = backend.object_dir(&addr());
        for dir in [
            root.clone(),
            root.join("heads"),
            head.clone(),
            head.join("v"),
            root.join("logs"),
            backend.log_dir(&addr()),
            root.join("objects"),
            objects.clone(),
            objects.join(hex(&log.0)),
            objects.join(hex(&log.0)).join("pages"),
            root.join("setup"),
            root.join("setup-plain"),
        ] {
            assert_mode(&dir, 0o700);
        }
        for file in [
            head.join("head"),
            head.join("v/1"),
            head.join("v/2"),
            backend.log_path(&addr(), &log),
            objects.join(&object),
            root.join("setup/aa"),
            root.join("setup-plain/bb"),
            cut,
        ] {
            assert_mode(&file, 0o600);
        }
    }
    assert_mode(&parent.join("session-store-moves"), 0o700);
    assert_mode(&parent.join("session-store-moves/staging"), 0o700);
    // Creation must not narrow a shared ancestor that the backend did not create.
    assert_mode(&parent, 0o755);
    std::fs::remove_dir_all(&parent).unwrap();
}

#[cfg(unix)]
#[test]
fn local_backend_temporary_files_are_owner_only_before_rename() {
    let root = root();
    for (folder, temp, name, is_head) in [
        ("heads/session", "head.tmp", "head", true),
        ("heads/session/v", ".1.tmp", "1", true),
        ("setup", ".aa.123.tmp", "aa", false),
        ("setup-plain", ".bb.456.tmp", "bb", false),
    ] {
        let dir = root.join(folder);
        // A directory at the destination makes rename fail, leaving the temporary file intact.
        std::fs::create_dir_all(dir.join(name)).unwrap();
        let result = if is_head {
            super::write_head_file(&dir, temp, name, 1, b"private")
        } else {
            super::write_file(&dir, temp, name, &[b"private"])
        };
        assert!(result.is_err(), "rename onto a directory must fail");
        let path = dir.join(temp);
        assert_mode(&path, 0o600);
        let expected = if is_head {
            [1_u64.to_le_bytes().as_slice(), b"private"].concat()
        } else {
            b"private".to_vec()
        };
        assert_eq!(std::fs::read(path).unwrap(), expected);
    }
    std::fs::remove_dir_all(&root).unwrap();
}
