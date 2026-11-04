//! broca's session storage format.
//!
//! The design is `.cortexkit/internal-docs/designs/session-storage/03-design.md` (internal). In
//! short: each session is a small head, one open log holding the active window, sealed segments
//! of older history, and shared setup parts, behind a backend interface, all encrypted.
//!
//! This crate is built standalone. It depends on none of today's storage crates (`broca-wal`,
//! `broca-store`) or on the run loop, and it never will: the converter crate is the only code
//! that reads both formats. Where the two would meet, callers adapt to this crate's state.
//!
//! Modules so far:
//! - `codec`: the tagged binary encoding every record, page and part is written in.
//! - `content`: encodings for the renderer's message and content types.
//! - `record`: the records of a session log, in two streams.
//! - `frame`: how one record sits in an open log, encrypted.
//! - `fold`: rebuilding a session's state from its records, one fold per stream.
//! - `log`: the open log, written in all-or-nothing batches and read with its end classified.
//! - `checkpoint`: the state a log's records fold onto, carried in its first batch.
//! - `keys`: session data keys, wrapped under the deployment key.
//! - `head`: the session head, the one small mutable object that names everything else.
//! - `backend`: where a session's bytes live: memory for tests, and local files.
//! - `session`: creating, loading and appending to a session.
//! - `prune`: removing opaque reasoning from records that leave the open log.
//! - `segment`: sealed segments, the immutable objects records move to when they leave the log.

pub mod backend;
pub mod checkpoint;
pub mod codec;
pub mod content;
pub mod fold;
pub mod frame;
pub mod head;
pub mod keys;
pub mod log;
pub mod prune;
pub mod record;
pub mod segment;
pub mod session;
