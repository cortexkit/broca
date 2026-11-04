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

pub mod codec;
pub mod content;
pub mod frame;
pub mod record;
