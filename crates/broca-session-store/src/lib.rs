//! broca's session storage format.
//!
//! The design is `.cortexkit/internal-docs/designs/session-storage/03-design.md` (internal). In
//! short: each session is a small head, one open log holding the active window, sealed segments
//! of older history, and shared setup parts, behind a backend interface. Sessions are encrypted
//! unless the deployment chose plain mode for them when they were created (see `keys`).
//!
//! This crate is built standalone. It depends on none of today's storage crates (`broca-wal`,
//! `broca-store`) or on the run loop, and it never will: the converter crate is the only code
//! that reads both formats. Where the two would meet, callers adapt to this crate's state.
//!
//! Modules so far:
//! - `codec`: the tagged binary encoding every record, page and part is written in.
//! - `content`: encodings for the renderer's message and content types.
//! - `record`: the records of a session log, in two streams.
//! - `frame`: how one record sits in an open log, sealed in its session's mode.
//! - `fold`: rebuilding a session's state from its records, one fold per stream.
//! - `log`: the open log, written in all-or-nothing batches and read with its end classified.
//! - `damage`: reading a damaged log: every frame that still authenticates, and what may be lost.
//! - `checkpoint`: the state a log's records fold onto, carried in its first batch.
//! - `keys`: session data keys, wrapped under the deployment key, and the cipher mode.
//! - `head`: the session head, the one small mutable object that names everything else.
//! - `backend`: where a session's bytes live: memory for tests, and local files.
//! - `session`: creating, loading and appending to a session.
//! - `prune`: removing opaque reasoning from records that leave the open log.
//! - `segment`: sealed segments, the immutable objects records move to when they leave the log.
//! - `directory`: which sealed segment holds each record sequence number, as a small tree of
//!   immutable pages.
//! - `setup`: the setup store, where the parts runs are started with live once per deployment.
//! - `idindex`: which sealed ledger records name a run id, send id, call key or other id.

/// Turns test functions that take a `keys::Cipher` into tests run once in each mode: each named
/// function `f` becomes the tests `f::encrypted` and `f::plain`. A store test runs in both modes
/// in every normal test run, so the plain mode is tested whenever the encrypted one is. A test
/// function left out of the list is never called, which the build refuses as dead code, so no
/// test is silently dropped.
#[cfg(test)]
macro_rules! cipher_tests {
    ($($name:ident),+ $(,)?) => {
        $(
            mod $name {
                #[test]
                fn encrypted() {
                    super::$name($crate::keys::Cipher::XChaCha20Poly1305);
                }

                #[test]
                fn plain() {
                    super::$name($crate::keys::Cipher::None);
                }
            }
        )+
    };
}

pub mod backend;
pub mod checkpoint;
pub mod codec;
pub mod content;
pub mod damage;
pub mod directory;
pub mod fold;
pub mod frame;
/// Decode entry points for the fuzz targets in `fuzz/` and the tests that replay their inputs.
/// Built only for tests and under `cargo fuzz`, which sets `--cfg fuzzing`.
#[cfg(any(test, fuzzing))]
pub mod fuzzing;
pub mod head;
pub mod idindex;
pub mod keys;
pub mod log;
pub mod prune;
pub mod record;
pub mod segment;
pub mod session;
pub mod setup;

// The crate's tests count what each thread allocates, so the fuzzing checks can hold decoders to
// allocations proportional to their input.
#[cfg(test)]
#[global_allocator]
static ALLOCATOR: fuzzing::CountingAlloc = fuzzing::CountingAlloc;
