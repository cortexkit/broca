use std::{cell::RefCell, io, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    BeforeFileSync,
    BeforeDirectorySync(PathBuf),
    FileSync,
    BeforePublish(PathBuf),
    Published(PathBuf),
    DirectorySync(PathBuf),
}

/// Observe the actual handle's successful sync, rather than the caller's intent
/// to sync. Skipping a helper's sync_all call therefore produces no success event.
pub(crate) struct SyncFile<'a> {
    inner: &'a std::fs::File,
    event: Event,
}
impl<'a> SyncFile<'a> {
    pub(crate) fn new(inner: &'a std::fs::File, event: Event) -> Self {
        Self { inner, event }
    }
    pub(crate) fn sync_all(&self) -> io::Result<()> {
        match &self.event {
            Event::FileSync => observe(Event::BeforeFileSync)?,
            Event::DirectorySync(path) => observe(Event::BeforeDirectorySync(path.clone()))?,
            _ => unreachable!("only file and directory syncs wrap filesystem handles"),
        }
        self.inner.sync_all()?;
        observe(self.event.clone())
    }
}
type Hook = Box<dyn FnMut(Event) -> io::Result<()>>;
thread_local! {
    static HOOK: RefCell<Option<Hook>> = RefCell::new(None);
}
pub struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        HOOK.with(|hook| *hook.borrow_mut() = None);
    }
}
pub fn install(hook: impl FnMut(Event) -> io::Result<()> + 'static) -> Guard {
    HOOK.with(|slot| {
        assert!(slot.borrow().is_none());
        *slot.borrow_mut() = Some(Box::new(hook));
    });
    Guard
}
pub fn observe(event: Event) -> io::Result<()> {
    HOOK.with(|slot| match slot.borrow_mut().as_mut() {
        Some(hook) => hook(event),
        None => Ok(()),
    })
}
