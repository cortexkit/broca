use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

use serde::Serialize;

use crate::SAMPLE_BYTES;

/// Frames kept per sampled stack, counting the allocator's own few at the top.
const DEPTH: usize = 48;
/// The site of a block that wasn't sampled.
const UNSAMPLED: usize = usize::MAX;

thread_local! {
    /// Set while this thread runs the profiler's own code, whose allocations are never sampled
    /// and which must not reenter itself.
    static BUSY: Cell<bool> = const { Cell::new(false) };
    /// Bytes this thread may still allocate before its next sample; negative until first drawn.
    static UNTIL_SAMPLE: Cell<i64> = const { Cell::new(-1) };
    /// This thread's random state for drawing sample gaps; zero until seeded.
    static RANDOM: Cell<u64> = const { Cell::new(0) };
}

/// Every block's requested bytes and count, sampled or not: what the estimates are read against.
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_BLOCKS: AtomicUsize = AtomicUsize::new(0);
/// Tells threads' random seeds apart.
static SEEDS: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);

struct Guard;

impl Guard {
    fn enter() -> Option<Self> {
        BUSY.try_with(|busy| (!busy.replace(true)).then_some(Self))
            .ok()
            .flatten()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let _ = BUSY.try_with(|busy| busy.set(false));
    }
}

/// What every block carries in front of the bytes it hands out: its sampled site, or
/// [`UNSAMPLED`], and the bytes the sample stands for.
#[repr(C)]
struct Header {
    site: usize,
    weight: usize,
}

#[derive(Serialize)]
struct Site {
    #[serde(serialize_with = "serialize_stack")]
    stack: [usize; DEPTH],
    /// Estimated bytes this site holds now.
    bytes: u64,
    /// Sampled blocks of this site still live.
    blocks: u64,
    /// Estimated bytes this site has allocated since the process started.
    allocated: u64,
}

fn serialize_stack<S: serde::Serializer>(
    stack: &[usize; DEPTH],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let len = stack.iter().position(|frame| *frame == 0).unwrap_or(DEPTH);
    stack[..len].serialize(serializer)
}

#[derive(Default)]
struct State {
    ids: HashMap<[usize; DEPTH], usize>,
    sites: Vec<Site>,
}

impl State {
    fn allocate(&mut self, stack: [usize; DEPTH], weight: usize) -> usize {
        let id = *self.ids.entry(stack).or_insert_with(|| {
            self.sites.push(Site {
                stack,
                bytes: 0,
                blocks: 0,
                allocated: 0,
            });
            self.sites.len() - 1
        });
        let site = &mut self.sites[id];
        site.bytes += weight as u64;
        site.blocks += 1;
        site.allocated += weight as u64;
        id
    }

    fn free(&mut self, id: usize, weight: usize) {
        let site = &mut self.sites[id];
        site.bytes -= weight as u64;
        site.blocks -= 1;
    }
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();

fn state() -> &'static Mutex<State> {
    STATE.get_or_init(|| Mutex::new(State::default()))
}

/// A gap to the next sample, drawn from the exponential distribution whose mean is
/// [`SAMPLE_BYTES`], so samples fall at random points of the allocation stream rather than in
/// step with a repeating allocation pattern.
fn draw() -> i64 {
    let bits = RANDOM
        .try_with(|random| {
            let mut x = random.get();
            if x == 0 {
                x = SEEDS.fetch_add(0x9e37_79b9_7f4a_7c15, Relaxed) | 1;
            }
            // xorshift64
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            random.set(x);
            x
        })
        .unwrap_or(1);
    let uniform = ((bits >> 11) as f64 + 1.0) / (1u64 << 53) as f64;
    (-uniform.ln() * SAMPLE_BYTES as f64) as i64
}

/// The bytes a sampled block of `size` stands for. A block is sampled with probability
/// `1 - exp(-size / SAMPLE_BYTES)`, so dividing its size by that keeps each site's total an
/// unbiased estimate of what it holds.
fn estimate(size: usize) -> usize {
    let chance = 1.0 - (-(size as f64) / SAMPLE_BYTES as f64).exp();
    ((size as f64 / chance) as usize).max(size)
}

/// Whether an allocation of `size` is sampled, and if so its site and estimate.
fn sample(size: usize) -> (usize, usize) {
    let Some(_guard) = Guard::enter() else {
        return (UNSAMPLED, 0);
    };
    let taken = UNTIL_SAMPLE
        .try_with(|left| {
            let mut gap = left.get();
            if gap < 0 {
                gap = draw();
            }
            gap -= size as i64;
            if gap > 0 {
                left.set(gap);
                return false;
            }
            left.set(draw());
            true
        })
        .unwrap_or(false);
    if !taken {
        return (UNSAMPLED, 0);
    }
    let weight = estimate(size);
    let id = state().lock().unwrap().allocate(stack(), weight);
    (id, weight)
}

fn unsample(header: &Header) {
    if header.site == UNSAMPLED {
        return;
    }
    // A sampled block is never freed by the profiler's own code, which only frees its own,
    // unsampled blocks; without the guard there is nothing safe to do but leave the estimate.
    if let Some(_guard) = Guard::enter() {
        state().lock().unwrap().free(header.site, header.weight);
    }
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
    // The unwinder calls back synchronously with the Trace passed to _Unwind_Backtrace.
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
        LIVE_BYTES.fetch_add(layout.size(), Relaxed);
        LIVE_BLOCKS.fetch_add(1, Relaxed);
        let (site, weight) = sample(layout.size());
        unsafe {
            base.cast::<Header>().write(Header { site, weight });
            base.add(offset)
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let (combined, offset) = extended(layout);
        let base = unsafe { ptr.sub(offset) };
        unsample(unsafe { &*base.cast::<Header>() });
        LIVE_BYTES.fetch_sub(layout.size(), Relaxed);
        LIVE_BLOCKS.fetch_sub(1, Relaxed);
        unsafe { System.dealloc(base, combined) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let (old, offset) = extended(layout);
        let (new, _) = extended(Layout::from_size_align(size, layout.align()).unwrap());
        let before = unsafe { ptr.sub(offset) };
        // The header's site may be freed below, so read it before the block can move.
        let header = unsafe { before.cast::<Header>().read() };
        let base = unsafe { System.realloc(before, old, new.size()) };
        if base.is_null() {
            return base;
        }
        // A resized block counts as the old one freed and a new one allocated, so a buffer
        // sampled small and grown large is weighed by the size it has now.
        unsample(&header);
        LIVE_BYTES.fetch_add(size, Relaxed);
        LIVE_BYTES.fetch_sub(layout.size(), Relaxed);
        let (site, weight) = sample(size);
        unsafe {
            base.cast::<Header>().write(Header { site, weight });
            base.add(offset)
        }
    }
}

#[derive(Serialize)]
struct Snapshot<'a> {
    /// The mean bytes between samples the estimates were taken with.
    sample_bytes: u64,
    /// Where the executable is loaded, for symbolizing the stacks with the same binary.
    image_base: usize,
    /// Every live block's requested bytes and count, counted exactly.
    live_bytes: usize,
    live_blocks: usize,
    /// The sum of the sites' estimates, to read against `live_bytes`.
    estimated_live_bytes: u64,
    sites: Vec<&'a Site>,
}

#[cfg(target_os = "macos")]
fn image_base() -> usize {
    extern "C" {
        fn _dyld_get_image_header(index: u32) -> *const c_void;
    }
    // Image 0 is the main executable.
    unsafe { _dyld_get_image_header(0) as usize }
}

#[cfg(target_os = "linux")]
fn image_base() -> usize {
    // The first mapping of the executable is its load address.
    let exe = std::fs::read_link("/proc/self/exe").unwrap_or_default();
    std::fs::read_to_string("/proc/self/maps")
        .unwrap_or_default()
        .lines()
        .find(|line| line.ends_with(&*exe.to_string_lossy()))
        .and_then(|line| line.split('-').next())
        .and_then(|start| usize::from_str_radix(start, 16).ok())
        .unwrap_or(0)
}

fn snapshot() -> Vec<u8> {
    let state = state().lock().unwrap();
    let sites: Vec<&Site> = state.sites.iter().filter(|site| site.bytes > 0).collect();
    serde_json::to_vec(&Snapshot {
        sample_bytes: SAMPLE_BYTES,
        image_base: image_base(),
        live_bytes: LIVE_BYTES.load(Relaxed),
        live_blocks: LIVE_BLOCKS.load(Relaxed),
        estimated_live_bytes: sites.iter().map(|site| site.bytes).sum(),
        sites,
    })
    .expect("heap snapshot")
}

pub(crate) fn start(dir: std::path::PathBuf) {
    if let Err(error) = std::fs::create_dir_all(&dir) {
        eprintln!("heap-profile: cannot create {}: {error}", dir.display());
        return;
    }
    let spawned = std::thread::Builder::new()
        .name("heap-profile".into())
        .spawn(move || {
            // This thread only answers requests; none of its allocations are sampled.
            let _guard = Guard::enter();
            loop {
                for path in std::fs::read_dir(&dir)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "request"))
                {
                    let pending = path.with_extension("pending");
                    let written = std::fs::write(&pending, snapshot())
                        .and_then(|()| std::fs::rename(&pending, path.with_extension("json")))
                        .and_then(|()| std::fs::remove_file(&path));
                    if let Err(error) = written {
                        eprintln!("heap-profile: {}: {error}", path.display());
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
        });
    if let Err(error) = spawned {
        eprintln!("heap-profile: cannot start its thread: {error}");
    }
}

#[cfg(test)]
mod tests;
