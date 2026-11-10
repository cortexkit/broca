use std::{io, path::Path};

/// Directory handles are syncable on Unix. On other platforms this function
/// deliberately performs no directory I/O rather than trying to open a directory.
pub fn sync_dir(path: &Path) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    #[cfg(unix)]
    {
        #[cfg(any(test, feature = "test-io-trace"))]
        fail_if_injected(path)?;
        crate::fs_ops::sync_dir(path)
    }
    #[cfg(not(unix))]
    {
        crate::fs_ops::sync_dir(path)
    }
}

#[cfg(all(any(test, feature = "test-io-trace"), unix))]
thread_local! {
    static FAILURE: std::cell::RefCell<Option<std::path::PathBuf>> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(any(test, feature = "test-io-trace"), unix))]
fn fail_if_injected(path: &Path) -> io::Result<()> {
    if FAILURE.with(|fault| fault.borrow().as_deref() == Some(path)) {
        return Err(io::Error::from_raw_os_error(5));
    }
    Ok(())
}

#[cfg(all(any(test, feature = "test-io-trace"), unix))]
#[doc(hidden)]
pub fn with_failure<T>(path: &Path, action: impl FnOnce() -> T) -> T {
    struct Restore(Option<std::path::PathBuf>);
    impl Drop for Restore {
        fn drop(&mut self) {
            FAILURE.with(|fault| *fault.borrow_mut() = self.0.take());
        }
    }
    let previous = FAILURE.with(|fault| fault.replace(Some(path.to_path_buf())));
    let _restore = Restore(previous);
    action()
}
