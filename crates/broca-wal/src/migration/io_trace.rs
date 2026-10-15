use std::{cell::RefCell, io, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    FileSync,
    BeforePublish(PathBuf),
    Published(PathBuf),
    DirectorySync(PathBuf),
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
