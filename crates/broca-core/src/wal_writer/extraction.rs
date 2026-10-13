use broca_owner_only::OpenOptions;
#[cfg(unix)]
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
type Observer = Box<dyn FnMut(&str)>;

#[cfg(test)]
thread_local! {
    pub(super) static OBSERVE: std::cell::RefCell<Option<Observer>> =
        const { std::cell::RefCell::new(None) };
}

fn observed(_operation: &str) {
    #[cfg(test)]
    OBSERVE.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(_operation);
        }
    });
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    // As with WAL creation, non-Unix durability covers file contents, not the directory
    // entry. A lost rename falls back to the intact archive; subsequent appends can be
    // lost. This inherited platform exposure is bounded by the archived transcript.
    Ok(())
}

/// Called only by append admission, with the session lease held and no live WAL open.
pub(super) fn restore(path: &Path, payload: &[u8]) -> io::Result<()> {
    observed("resolved");
    let parent = path.parent().expect("canonical WAL has a parent");
    // A restored state tree may contain archives without a wal directory. Sync each
    // newly created directory's parent too, so the final rename is actually reachable.
    let mut missing = Vec::new();
    let mut directory = parent;
    while !directory.as_os_str().is_empty() && !directory.try_exists()? {
        missing.push(directory.to_path_buf());
        directory = directory.parent().unwrap_or_else(|| Path::new(""));
    }
    broca_owner_only::create_dir_all(parent)?;
    observed("directory created");
    for directory in missing.iter().rev() {
        let ancestor = directory
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(ancestor)?;
        observed("ancestor synced");
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let (temporary, mut file) = loop {
        let temporary = parent.join(format!(
            ".extract-{}-{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        observed("temp created");
        for bytes in payload.chunks(4096) {
            file.write_all(bytes)?;
            observed("temp written");
        }
        file.sync_all()?;
        observed("temp synced");
        drop(file);
        // Before rename, live is absent and readers serve the intact archive. After
        // rename, live-wins sees a COMPLETE file, never a partially written transcript.
        // The temp shares the canonical directory so this cannot cross filesystems.
        std::fs::rename(&temporary, path)?;
        observed("renamed");
        // Existing FileWal opens set dir_needs_sync=false: no later append barrier
        // will make this rename durable. Unix sync failures must abort admission.
        sync_directory(parent)?;
        observed("parent synced");
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    #[test]
    fn windows_extraction_temporary_and_replacement_are_private() {
        let fixture = BroadParent::new("extraction");
        let path = fixture.path.join("nested/session.wal");
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || restore(&path, b"archive payload").unwrap(),
        );
        assert_eq!(
            seen.get(),
            1,
            "temporary extraction must be inspected before rename"
        );
        assert_private(&path);
        assert_private(&fixture.path.join("nested"));
        assert_eq!(std::fs::read(&path).unwrap(), b"archive payload");
    }
}
