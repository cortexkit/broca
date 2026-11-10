use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Io {
    CreateDir,
    Open,
    Lock,
    Read,
    FileSync,
    Rename,
    DirSync,
}

#[derive(Default)]
pub struct Trace {
    pub events: Vec<Io>,
    pub cut: Option<Io>,
    pub crash: bool,
}

thread_local! {
    pub static TRACE: RefCell<Option<Trace>> = const { RefCell::new(None) };
}

pub(crate) fn observe(event: Io) -> std::io::Result<()> {
    TRACE.with_borrow_mut(|trace| {
        if let Some(trace) = trace {
            trace.events.push(event);
            if trace.cut == Some(event) {
                if trace.crash {
                    std::process::exit(86);
                }
                return Err(std::io::Error::other(format!("floor crash cut: {event:?}")));
            }
        }
        Ok(())
    })
}

pub fn traced<T>(cut: Option<Io>, action: impl FnOnce() -> T) -> (T, Vec<Io>) {
    struct Restore(Option<Trace>);
    impl Drop for Restore {
        fn drop(&mut self) {
            TRACE.with_borrow_mut(|trace| *trace = self.0.take());
        }
    }
    let _restore = Restore(TRACE.replace(Some(Trace {
        events: Vec::new(),
        cut,
        crash: false,
    })));
    let result = action();
    let events = TRACE.with_borrow_mut(|trace| std::mem::take(&mut trace.as_mut().unwrap().events));
    (result, events)
}
