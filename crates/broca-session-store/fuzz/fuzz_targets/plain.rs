#![no_main]

use broca_session_store::fuzzing;

// Counts what each thread holds, so the entry point can bound the memory one decode takes.
#[global_allocator]
static ALLOCATOR: fuzzing::CountingAlloc = fuzzing::CountingAlloc;

libfuzzer_sys::fuzz_target!(|data: &[u8]| fuzzing::plain(data));
