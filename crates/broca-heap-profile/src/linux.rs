use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

const DEPTH: usize = 64;
const UNTRACKED: usize = usize::MAX;
const PEAK_INTERVAL: usize = 4 * 1024 * 1024;

thread_local! {
    static BUSY: Cell<bool> = const { Cell::new(false) };
}

struct Guard;

impl Guard {
    fn enter() -> Option<Self> {
        BUSY.try_with(|busy| {
            if busy.get() {
                None
            } else {
                busy.set(true);
                Some(Self)
            }
        })
        .ok()
        .flatten()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = BUSY.try_with(|busy| busy.set(false));
    }
}

#[repr(C)]
struct Header {
    site: usize,
}

#[derive(Default, Serialize)]
struct Counts {
    bytes: usize,
    blocks: usize,
    allocated_bytes: u64,
}

#[derive(Serialize)]
struct Site {
    #[serde(serialize_with = "serialize_stack")]
    stack: [usize; DEPTH],
    live: Counts,
    at_peak_bytes: usize,
    at_peak_blocks: usize,
}

fn serialize_stack<S: serde::Serializer>(
    stack: &[usize; DEPTH],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    stack.as_slice().serialize(serializer)
}

#[derive(Default, Serialize)]
struct State {
    #[serde(skip)]
    ids: HashMap<[usize; DEPTH], usize>,
    sites: Vec<Site>,
    live_bytes: usize,
    live_blocks: usize,
    exact_peak_bytes: usize,
    sampled_peak_bytes: usize,
}

impl State {
    fn peak(&mut self) {
        self.exact_peak_bytes = self.exact_peak_bytes.max(self.live_bytes);
        // Copying every site's counters on every allocation would dominate the load.
        // The captured peak is a simultaneous heap, within 4 MiB of the high water.
        if self.live_bytes >= self.sampled_peak_bytes + PEAK_INTERVAL {
            self.sampled_peak_bytes = self.live_bytes;
            for site in &mut self.sites {
                site.at_peak_bytes = site.live.bytes;
                site.at_peak_blocks = site.live.blocks;
            }
        }
    }

    fn allocate(&mut self, stack: [usize; DEPTH], size: usize) -> usize {
        let id = match self.ids.get(&stack) {
            Some(id) => *id,
            None => {
                let id = self.sites.len();
                self.ids.insert(stack, id);
                self.sites.push(Site {
                    stack,
                    live: Counts::default(),
                    at_peak_bytes: 0,
                    at_peak_blocks: 0,
                });
                id
            }
        };
        let live = &mut self.sites[id].live;
        live.bytes += size;
        live.blocks += 1;
        live.allocated_bytes += size as u64;
        self.live_bytes += size;
        self.live_blocks += 1;
        self.peak();
        id
    }

    fn free(&mut self, id: usize, size: usize) {
        if id != UNTRACKED {
            self.sites[id].live.bytes -= size;
            self.sites[id].live.blocks -= 1;
            self.live_bytes -= size;
            self.live_blocks -= 1;
        }
    }
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();

fn state() -> &'static Mutex<State> {
    STATE.get_or_init(|| Mutex::new(State::default()))
}

extern "C" {
    fn _Unwind_Backtrace(
        callback: extern "C" fn(*mut c_void, *mut c_void) -> c_int,
        argument: *mut c_void,
    ) -> c_int;
    fn _Unwind_GetIP(context: *mut c_void) -> usize;
}

struct Trace {
    frames: [usize; DEPTH],
    len: usize,
}

extern "C" fn frame(context: *mut c_void, argument: *mut c_void) -> c_int {
    // libgcc calls back synchronously with the Trace passed to _Unwind_Backtrace.
    let trace = unsafe { &mut *argument.cast::<Trace>() };
    if trace.len == DEPTH {
        return 5; // _URC_END_OF_STACK
    }
    trace.frames[trace.len] = unsafe { _Unwind_GetIP(context) }.saturating_sub(1);
    trace.len += 1;
    0 // _URC_NO_REASON
}

fn stack() -> [usize; DEPTH] {
    let mut trace = Trace {
        frames: [0; DEPTH],
        len: 0,
    };
    unsafe { _Unwind_Backtrace(frame, (&mut trace as *mut Trace).cast()) };
    trace.frames
}

fn extended(layout: Layout) -> (Layout, usize) {
    let (combined, offset) = Layout::new::<Header>()
        .extend(layout)
        .expect("allocation layout");
    (combined.pad_to_align(), offset)
}

struct Allocator;

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (combined, offset) = extended(layout);
        let base = unsafe { System.alloc(combined) };
        if base.is_null() {
            return base;
        }
        let id = if let Some(_guard) = Guard::enter() {
            state().lock().unwrap().allocate(stack(), layout.size())
        } else {
            UNTRACKED
        };
        unsafe {
            base.cast::<Header>().write(Header { site: id });
            base.add(offset)
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (combined, offset) = extended(layout);
        let base = unsafe { ptr.sub(offset) };
        let id = unsafe { (*base.cast::<Header>()).site };
        if id != UNTRACKED {
            let _guard = Guard::enter();
            state().lock().unwrap().free(id, layout.size());
        }
        unsafe { System.dealloc(base, combined) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let (old, offset) = extended(layout);
        let (new, _) = extended(Layout::from_size_align(size, layout.align()).unwrap());
        let base = unsafe { System.realloc(ptr.sub(offset), old, new.size()) };
        if base.is_null() {
            return base;
        }
        let id = unsafe { (*base.cast::<Header>()).site };
        if id != UNTRACKED {
            let _guard = Guard::enter();
            let mut state = state().lock().unwrap();
            state.free(id, layout.size());
            let live = &mut state.sites[id].live;
            live.bytes += size;
            live.blocks += 1;
            live.allocated_bytes += size as u64;
            state.live_bytes += size;
            state.live_blocks += 1;
            state.peak();
        }
        unsafe { base.add(offset) }
    }
}

pub fn start() {
    let Some(dir) = std::env::var_os("BROCA_HEAP_PROFILE_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).expect("heap profile directory");
    std::thread::Builder::new()
        .name("heap-profile".into())
        .spawn(move || loop {
            for entry in std::fs::read_dir(&dir).expect("heap profile requests") {
                let path = entry.expect("heap profile entry").path();
                if path.extension().is_some_and(|ext| ext == "request") {
                    let _guard = Guard::enter().expect("snapshot recursion");
                    if path.file_stem().is_some_and(|name| name == "reset") {
                        let mut state = state().lock().unwrap();
                        state.exact_peak_bytes = state.live_bytes;
                        state.sampled_peak_bytes = state.live_bytes;
                        for site in &mut state.sites {
                            site.at_peak_bytes = site.live.bytes;
                            site.at_peak_blocks = site.live.blocks;
                        }
                    }
                    let json =
                        serde_json::to_vec(&*state().lock().unwrap()).expect("heap snapshot");
                    let pending = path.with_extension("pending");
                    std::fs::write(&pending, json).expect("write heap snapshot");
                    std::fs::rename(pending, path.with_extension("json"))
                        .expect("publish heap snapshot");
                    std::fs::remove_file(path).expect("remove heap request");
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        })
        .expect("heap snapshot thread");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_allocation_and_cross_thread_free() {
        let mut state = State::default();
        let id = state.allocate([17; DEPTH], 4096);
        assert_eq!(state.live_bytes, 4096);
        assert_eq!(state.sites[id].live.blocks, 1);
        state.free(id, 4096);
        assert_eq!(state.live_bytes, 0);
        assert_eq!(state.live_blocks, 0);
        let buffer = vec![7u64; 1024];
        std::thread::spawn(move || drop(buffer)).join().unwrap();
    }

    #[test]
    fn realloc_preserves_alignment_and_original_site() {
        let layout = Layout::from_size_align(4096, 64).unwrap();
        let ptr = unsafe { ALLOCATOR.alloc(layout) };
        assert!(!ptr.is_null());
        assert_eq!(ptr as usize % 64, 0);
        let id = unsafe { (*ptr.sub(extended(layout).1).cast::<Header>()).site };
        let resized = unsafe { ALLOCATOR.realloc(ptr, layout, 8192) };
        assert!(!resized.is_null());
        assert_eq!(resized as usize % 64, 0);
        assert_eq!(state().lock().unwrap().sites[id].live.bytes, 8192);
        unsafe { ALLOCATOR.dealloc(resized, Layout::from_size_align(8192, 64).unwrap()) };
        assert_eq!(state().lock().unwrap().sites[id].live.bytes, 0);
    }

    #[test]
    fn nested_allocator_entries_remain_untracked() {
        let guard = Guard::enter().unwrap();
        assert!(Guard::enter().is_none());
        assert!(BUSY.with(Cell::get));
        drop(guard);
        assert!(!BUSY.with(Cell::get));
    }

    #[test]
    fn peak_counters_describe_one_simultaneous_heap() {
        let mut state = State::default();
        let a = state.allocate([1; DEPTH], PEAK_INTERVAL);
        state.free(a, PEAK_INTERVAL);
        let b = state.allocate([2; DEPTH], 2 * PEAK_INTERVAL);
        assert_eq!(state.sites[a].at_peak_bytes, 0);
        assert_eq!(state.sites[b].at_peak_bytes, 2 * PEAK_INTERVAL);
        assert_eq!(state.sampled_peak_bytes, 2 * PEAK_INTERVAL);
    }
}
